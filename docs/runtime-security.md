# Runtime dependency maintenance

The service stays on Java 21 and Spring Boot 3.5. Security versions newer than
that parent are explicitly managed in `pom.xml`: Jackson BOM 2.21.8 (annotations
2.21), Tomcat 10.1.60, Log4j 2.25.5 and the maintained `at.yawk.lz4` 1.11.4 codec.
Kafka clients/Streams resolve to 3.9.2 and Spring Kafka to 3.3.16. Remove explicit
overrides only after the parent supplies an equivalent or newer compatible fix.
Do not restore the obsolete `org.lz4` artifact alongside its replacement.

The Docker runtime pins the supported Ubuntu Noble Temurin image by digest.
Until the matching official image is published, the Dockerfile replaces the
JRE with official Temurin 21.0.13+9, verified against fixed architecture-specific
SHA-256 checksums. The Noble image has no unused Pebble executable. CA support,
native libraries, timezone handling and UID 10001 remain in place. Replace this
bridge with a scanned immutable official image when available; updating only
the base digest must not silently downgrade the JRE. No application configuration
or message-schema changes are required.

## Remaining Spring MVC package findings

The compatible open-source Spring 6.2.19 dependency still matches
[CVE-2026-47884](https://spring.io/security/cve-2026-47884/) and
[CVE-2026-47890](https://spring.io/security/cve-2026-47890/). The former requires
XsltView with a catch-all implicit view; the latter requires SSE view fragments.
Runtime source has no application MVC controllers, XSLT views, SSE emitters,
view-fragment endpoints or view templates. `application.yaml` exposes only
Actuator health, info and Prometheus endpoints. Kafka JSON processing does not
invoke either MVC rendering feature.

This is a source-level applicability assessment, not a claim that the packages
are fixed and not an approved scanner suppression. Keep both findings visible.
The service maintainers must review this assessment by 2026-11-24, on any HTTP
controller/view change, or before promoting a new image. Resolve it through a
tested supported Boot 4 migration or supported Spring 6.2 backports; never place
Spring 7 underneath Boot 3 by overriding one jar.


The bundled default Java truststore follows Oracle's eMudhra root-removal
advice. The remaining emSign G1 alias is deleted only
after its exact SHA-256 fingerprint matches; the build then checks that all four
listed root subjects are absent. TLS certificates chaining only to these roots
are deliberately rejected. Application-provided truststores and explicit
`javax.net.ssl.trustStore` overrides retain their existing behavior and must be
audited separately; this image change does not sanitize a mounted truststore.

## Release validation

Run `mvn test` and `mvn -Dgroups=integration -DexcludedGroups= test` with Docker.
Local integration runs use `TESTCONTAINERS_RYUK_DISABLED=true`; fixtures bind
published Kafka and HTTP ports only to `127.0.0.1` and stop their containers.
The real-broker tests send LZ4-compressed records. Docker builds also run the
unit suite under Java 21 on Linux, including platform-specific checks.

Build the final image for its release platform, inspect `java --version`, save
the runtime dependency tree and scan the final filesystem without ignoring
unfixed findings. Record the image digest, scanner version/database timestamp
and all severities privately with the release evidence. Inspect shaded-library
metadata as well: an absent scanner entry is not proof that a bundled package
is absent. These checks do not replace platform/network hardening.

The AWS SDK modules are aligned at 2.57.3, whose internal shaded Jackson is
2.21.8. Updating the application Jackson BOM alone cannot update that copy.
Only synchronous Bedrock calls are used, so the unused `netty-nio-client`
transport is excluded; the SDK's synchronous Apache transport remains.
HttpClient 5.6.4 and HttpCore 5.4.3 match that SDK's intended baseline; explicit
Boot properties prevent its older BOM from downgrading those security fixes.
`BedrockTransportTest` makes a signed synthetic request to a localhost endpoint
with fixed test credentials and requires no AWS access.

The `openai_compatible` provider uses the JDK `java.net.http.HttpClient` and the
existing Jackson, so it adds no dependency. It speaks HTTP/1.1, follows no
redirects, uses no proxy and caps the response body it reads. `https` endpoints
are verified against the default truststore described above.
