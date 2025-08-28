package com.example.notification.config;

import com.example.notification.persistance.NotificationDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.*;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${application.instance-name}")
    private String applicationInstanceName;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, NotificationDocument.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, Object> factory =
                new DefaultKafkaConsumerFactory<>(props);
        factory.setValueDeserializer(new JsonDeserializer<>(Object.class, objectMapper));
        return factory;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(configProps);

        factory.setValueSerializer(new JsonSerializer<>(objectMapper));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(ConsumerFactory<String, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean
    public KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
        return new KafkaListenerEndpointRegistry();
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic("ws-notification", 1, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic("broadcast-notification", 1, (short) 1);
    }


    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
//
//    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
//
//        Map<String, Object> props = new HashMap<>();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "broadcast-notification-" + applicationInstanceName);
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        return new KafkaStreamsConfiguration(props);
//    }

//    @Bean
//    public KStream<String, NotificationMessage> forwardNotification(StreamsBuilder builder) {
//        KStream<String, NotificationMessage> stream = builder.stream("ws-notification",
//                Consumed.with(Serdes.String(), JsonSerdes.notification()));
//
//
//        stream.map((key, value) -> KeyValue.pair(value.getMessageId(), value))
//                .to("notification-event-source", Produced.with(Serdes.String(), JsonSerdes.notification()));
//
//        return stream;
//    }
//
//    @Bean
//    public KTable<Long, Map<String, NotificationMessagePair>> notificationPendingStreams(StreamsBuilder builder) {
//        return builder.stream("notification-event-source", Consumed.with(Serdes.String(), JsonSerdes.notification()))
//                .groupBy((messageId, notificationMessage) -> notificationMessage.getUserId(),
//                        Grouped.with(Serdes.Long(), JsonSerdes.notification()))
//                .aggregate(HashMap::new,
//                        (userId, message, aggregate) -> {
//                            log.info("notificationPendingStreams() - userId = {}, message = {}.", userId, message);
//                            if (message == null) {
//                                return aggregate;
//                            }
//
//                            if (aggregate.get(message.getMessageId()) == null) {
//                                var pair = new NotificationMessagePair();
//                                pair.add(message);
//
//                                aggregate.put(message.getMessageId(), pair);
//                            } else {
//                                var pair = aggregate.get(message.getMessageId());
//                                pair.add(message);
//
//                                if (pair.isDone()) {
//                                    aggregate.remove(message.getMessageId());
//                                }
//                            }
//
//                            return aggregate;
//                        },
//
//                        Materialized.<Long, Map<String, NotificationMessagePair>, KeyValueStore<Bytes, byte[]>>as("notification-store")
//                                .withKeySerde(Serdes.Long())
//                                .withValueSerde(JsonSerdes.notificationPairMap())
//                                .withRetention(Duration.ofDays(3))
//                );
//    }
}
