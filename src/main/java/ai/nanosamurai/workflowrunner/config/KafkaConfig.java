package ai.nanosamurai.workflowrunner.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory(WorkflowRunnerProperties props) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.kafka().bootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, props.kafka().consumerGroupId());

        // Brokers with TLS listeners need security.protocol=SSL. If we speak PLAINTEXT to a TLS broker,
        // the Kafka client can allocate huge buffers and OOM while parsing the TLS handshake.
        // Default to PLAINTEXT to keep local dev simple.
        if (props.kafka().securityProtocol() != null && !props.kafka().securityProtocol().isBlank()) {
            cfg.put("security.protocol", props.kafka().securityProtocol());
        }

        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        // Retry tiers can require long not_before delays (up to hours). Allow long processing.
        cfg.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 28_800_000); // 8h

        return new DefaultKafkaConsumerFactory<>(cfg, new StringDeserializer(), new ByteArrayDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> f = new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(consumerFactory);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        f.setConcurrency(1);
        return f;
    }

    @Bean
    public ProducerFactory<String, byte[]> producerFactory(WorkflowRunnerProperties props) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.kafka().bootstrapServers());

        if (props.kafka().securityProtocol() != null && !props.kafka().securityProtocol().isBlank()) {
            cfg.put("security.protocol", props.kafka().securityProtocol());
        }

        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        return new DefaultKafkaProducerFactory<>(cfg, new StringSerializer(), new ByteArraySerializer());
    }

    @Bean
    public KafkaTemplate<String, byte[]> kafkaTemplate(ProducerFactory<String, byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
