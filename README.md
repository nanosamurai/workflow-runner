# workflow-runner

Kafka consumer service that **executes LLM workflows** for the **nanosamurai** stack.
This is the workflow runner of the nanosamurai **Community Edition**.

It consumes JSON `WorkflowDispatchJob` messages produced upstream (for example
by the nanosamurai BFF) and emits JSON workflow result and audit/outcome events.

## Responsibilities

- Consume workflow jobs from Kafka (`workflow.dispatch` + retry tiers)
- Execute the model call on the job's provider: **AWS Bedrock** or an
  **OpenAI-compatible** server (Ollama, vLLM, llama.cpp server); see [LLM providers](#llm-providers)
- Enforce the runner output contract:
  - always produce `workflow.result.render.markdown`
  - optionally also `workflow.result.render.json` (structured payload)
- Kafka-based retries through delayed retry-tier topics (10s, 2m, 10m, 2h)
- Emit `workflow.outcome` for **every attempt** (success/retry/failure)
- DLQ on permanent failures or when max retry attempts are exceeded

Delivery semantics: **at-least-once**.

## Kafka topics

Inputs:

- `workflow.dispatch`
- `workflow.dispatch.retry.10s`
- `workflow.dispatch.retry.2m`
- `workflow.dispatch.retry.10m`
- `workflow.dispatch.retry.2h`

Outputs:

- `workflow.result`
- `workflow.outcome`
- `workflow.dispatch.dlq`

## Message contracts (v1)

### Input: `WorkflowDispatchJob`

Key: recommended `session_id` (preserves per-session ordering).

Value (JSON example):

```json
{
  "schema_version": 1,
  "workflow_run_id": "<uuid>",
  "tenant_id": "<uuid>",
  "session_id": "<uuid>",
  "workflow_id": "<uuid>",
  "trigger": {
    "type": "transcript.refined.segment",
    "track_id": "parakeet",
    "source_event_id": "<uuid>",
    "created_at": "2026-04-28T21:00:00.000Z"
  },
  "provider": {
    "type": "bedrock",
    "model_id": "anthropic.claude-3-5-sonnet",
    "params": {"temperature": 0.2, "max_tokens": 800}
  },
  "prompt": {"text": "... user prompt ..."},
  "input": {
    "transcript": {
      "kind": "refined.consolidated",
      "segments": [
        {"start_s": 1.0, "end_s": 2.0, "speaker": "SPEAKER_00", "text": "..."}
      ]
    }
  },
  "stream": {
    "source_uri": "wss://<bff-node>/ws/sessions/<session_id>",
    "source_node_id": "..."
  },
  "attempt": {"no": 0}
}
```

### Output: `workflow.result`

Value (JSON example):

```json
{
  "schema_version": 1,
  "workflow_run_id": "<uuid>",
  "tenant_id": "<uuid>",
  "session_id": "<uuid>",
  "workflow_id": "<uuid>",
  "trigger": {"type": "transcript.refined.segment", "track_id": "parakeet", "source_event_id": "<uuid>"},
  "status": "ok",
  "render": {
    "markdown": "# Summary\n\n- ...\n",
    "json": {"summary": "..."}
  },
  "provider": {"type": "bedrock", "model_id": "anthropic.claude-3-5-sonnet"},
  "usage": {"input_tokens": 1234, "output_tokens": 456},
  "created_at": "2026-04-28T21:00:01.000Z",
  "stream": {"source_uri": "...", "source_node_id": "..."}
}
```

On failure, `status=failed` and `error_code/error_detail` are populated.

### Source tracks

Transcript jobs require `trigger.track_id`, the source transcription track
selected in the workflow definition. Producers must set it and include only that
track's transcript in the job; jobs without it fail validation before a model is
invoked. The runner preserves the trigger in successful/failed results, attempt
outcomes, retries and DLQ records. `recording.finished` has no source track.
`input.transcript.kind` still distinguishes `refined.window`,
`refined.consolidated` and `final`.

One workflow definition selects one track, so consumers of `workflow.result` can
key results by `(session_id, workflow_id)`.

### Output: `workflow.outcome`

Operational/audit event emitted for every attempt.

```json
{
  "schema_version": 1,
  "workflow_run_id": "<uuid>",
  "tenant_id": "<uuid>",
  "session_id": "<uuid>",
  "workflow_id": "<uuid>",
  "attempt_no": 0,
  "status": "SUCCESS",
  "error_code": null,
  "error_detail": null,
  "latency_ms": 150,
  "retry_to_topic": null,
  "created_at": "2026-04-28T21:00:01.000Z"
}
```

## LLM providers

Each job is routed by `provider.type`:

| `provider.type` | Client |
|---|---|
| missing or blank | `bedrock` (backward compatible) |
| `bedrock` | AWS Bedrock `InvokeModel` (Anthropic messages shape); `params` are passed through |
| `openai_compatible` | `POST {WORKFLOW_RUNNER_OPENAI_BASE_URL}/chat/completions` |

Only the providers listed in `WORKFLOW_RUNNER_PROVIDERS` are enabled. A job for
any other type fails permanently with `PROVIDER_NOT_ENABLED` (failed result,
`FAILED_PERMANENT` outcome and DLQ) without a model call. Results report the
type that ran, so a job without a type shows `bedrock`. The AWS Bedrock client
is only created when `bedrock` is enabled, so a runner with only
`openai_compatible` needs no AWS region or credentials.

### `openai_compatible`

Works with Ollama, vLLM and the llama.cpp server. The runner sends a standard,
non-streaming chat completions request:

```json
{"temperature": 0.2, "max_tokens": 800,
 "model": "llama3.1:8b",
 "messages": [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}],
 "stream": false}
```

- Only `temperature`, `top_p`, `max_tokens`, `seed` and `stop` are taken from
  `provider.params`; other keys are dropped. `model`, `messages` and `stream`
  are set by the runner after the params, so a workflow definition can't
  override them.
- `provider.model_id` must be listed in `WORKFLOW_RUNNER_OPENAI_MODELS`;
  otherwise the job fails permanently with `MODEL_NOT_ALLOWED` before any call.
- The reply is read from `choices[0].message.content`; `usage.prompt_tokens`
  and `usage.completion_tokens` become `usage.input_tokens/output_tokens`.
- Errors: connect failures (`PROVIDER_UNAVAILABLE`), timeouts
  (`PROVIDER_TIMEOUT`) and HTTP 408, 429 and 5xx (`PROVIDER_UNAVAILABLE`) use the
  retry tiers. Other non-2xx statuses, including redirects, fail permanently with
  `PROVIDER_REJECTED` and only the status code as detail. A body that is not
  JSON, has no message content or exceeds 4 MiB fails with `PROVIDER_BAD_RESPONSE`.
- An empty or blank reply also fails permanently with `PROVIDER_BAD_RESPONSE`
  instead of storing an empty result. Reasoning models (for example `qwen3`,
  `gpt-oss`) return their thinking in a separate field that counts toward
  `max_tokens`; when it uses them all up, the detail says `finish_reason=length`
  and suggests raising `max_tokens` or choosing a model that reasons less.

Security:

- The endpoint comes only from operator configuration, never from a job, so a
  workflow definition can't point the runner at another URL (SSRF). The URL is
  validated at startup (absolute `http`/`https`, no credentials, query or fragment).
- Redirects are never followed and no HTTP proxy is used.
- Transcripts are sensitive and become prompt input: keep the server on an
  internal network (no host port, egress limited to it) and prefer `https` when
  an API key is set. The runner warns at startup when a key is sent over `http`.
- Prompts, transcripts, response bodies and the API key are never logged, and
  provider error bodies are not copied into results or outcomes.

## Output parsing policy

The runner *asks* the model to respond with strict JSON:

```json
{"markdown": "...", "json": {"...": "..."}}
```

If the model returns non-JSON, the runner falls back to treating the raw response as `render.markdown` and emits an outcome with:
- `status=SUCCESS`
- `error_code=OUTPUT_NOT_JSON`

## Retries + DLQ

Transient failures move the job through delayed retry-tier topics:

1. `workflow.dispatch`
2. `workflow.dispatch.retry.10s`
3. `workflow.dispatch.retry.2m`
4. `workflow.dispatch.retry.10m`
5. `workflow.dispatch.retry.2h`

The retry message contains `attempt.no` incremented and uses Kafka header:

- `X-Nanosamurai-Not-Before-EpochMs`

Retry consumers sleep until this timestamp before processing.

DLQ topic:
- `workflow.dispatch.dlq`

DLQ records include headers:
- `X-Nanosamurai-Dlq-Reason`
- `X-Nanosamurai-Attempt-No`

## Configuration

All configuration is in `src/main/resources/application.yaml` and can be overridden via env vars.

Key env vars:

- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `KAFKA_CONSUMER_GROUP_ID` (default `workflow-runner`)
- `KAFKA_SECURITY_PROTOCOL` (default `PLAINTEXT`; set to `SSL` for brokers with TLS listeners)

LLM providers (see [LLM providers](#llm-providers)); invalid settings stop the
runner at startup with a message naming the variable:

- `WORKFLOW_RUNNER_PROVIDERS` (default `bedrock`): comma list of enabled
  provider types, `bedrock` and/or `openai_compatible`. Unknown names are rejected.
- `WORKFLOW_RUNNER_OPENAI_BASE_URL`: required when `openai_compatible` is
  enabled, e.g. `http://ollama:11434/v1`; the runner POSTs to `{base}/chat/completions`.
- `WORKFLOW_RUNNER_OPENAI_MODELS`: required when `openai_compatible` is
  enabled; comma allow-list of model ids, e.g. `llama3.1:8b`.
- `WORKFLOW_RUNNER_OPENAI_API_KEY` (optional): sent as `Authorization: Bearer`
  only when set; never logged.
- `WORKFLOW_RUNNER_OPENAI_TIMEOUT_S` (default `300`): timeout per request in seconds.

LLM execution mode:

- `WORKFLOW_RUNNER_LLM_MODE`
  - unset (default): call the enabled providers
  - `stub`: local smoke-test stub that replaces **every enabled provider**, performs
    **no network calls** and generates deterministic output. Provider routing and
    `PROVIDER_NOT_ENABLED` still apply; the OpenAI settings and model allow-list are not used.

Topic overrides:

- `KAFKA_TOPIC_WORKFLOW_DISPATCH`
- `KAFKA_TOPIC_WORKFLOW_DISPATCH_RETRY_10S`
- `KAFKA_TOPIC_WORKFLOW_DISPATCH_RETRY_2M`
- `KAFKA_TOPIC_WORKFLOW_DISPATCH_RETRY_10M`
- `KAFKA_TOPIC_WORKFLOW_DISPATCH_RETRY_2H`
- `KAFKA_TOPIC_WORKFLOW_DISPATCH_DLQ`
- `KAFKA_TOPIC_WORKFLOW_RESULT`
- `KAFKA_TOPIC_WORKFLOW_OUTCOME`

Retry tuning:

- `WORKFLOW_MAX_ATTEMPTS` (default `5`)
- `WORKFLOW_RETRY_JITTER_MS` (default `250`)

## Build & run

See [runtime dependency maintenance](docs/runtime-security.md) for dependency
and JRE updates, release validation and the remaining Spring MVC assessment.

```bash
mvn test
mvn package
java -jar target/workflow-runner-*.jar
```

Integration tests (requires Docker):

```bash
mvn -Dgroups=integration -DexcludedGroups= test
```

## Docker

```bash
docker build -t nanosamurai/workflow-runner:dev .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -p 127.0.0.1:8080:8080 \
  nanosamurai/workflow-runner:dev
```

## CI/CD

GitHub Actions workflows:

- **CI** (`.github/workflows/ci.yml`) runs `mvn test` on pull requests to `master`.
- **Secret scan** (`.github/workflows/gitleaks.yml`) runs gitleaks over the git
  history and the current tree (with `.gitleaks-public-tree.toml`) on pull
  requests to `master`.
- **Publish** (`.github/workflows/publish-image.yml`) runs on pushes to `master`:
  it calls both workflows above as gates (CI then also does a Docker build smoke
  test), then builds and pushes the image to the GitHub Container Registry with
  an SPDX SBOM and provenance:

  `ghcr.io/nanosamurai/workflow-runner:sha-<commit>`

  It authenticates with the workflow's `GITHUB_TOKEN`; no repository secrets or
  variables are needed.

## Health & metrics

Spring Boot Actuator endpoints:

- `GET /actuator/health`
- `GET /actuator/health/liveness`
- `GET /actuator/health/readiness`
- `GET /actuator/prometheus`

## Logging / security notes

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).


- Do not log transcript bodies or model outputs at INFO.
- Current implementation logs only minimal identifiers at WARN on transient failures.
- Provider failures log `llm_provider_failed` with the provider type, error code and
  a short detail (status code or exception class), never request or response bodies.

Local stub mode notes:

- `WORKFLOW_RUNNER_LLM_MODE=stub` is intended only for local smoke tests.
- It must never be enabled in production environments.
