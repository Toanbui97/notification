package com.example.notification.config;

import com.example.notification.helper.JsonSerdes;
import com.example.notification.model.NotificationMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EnableKafka
@Configuration
@EnableKafkaStreams
public class KafkaConfiguration {

    @Bean
    public ConsumerFactory<String, NotificationMessage> consumerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, NotificationMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, NotificationMessage> factory =
                new DefaultKafkaConsumerFactory<>(props);
        factory.setValueDeserializer(new JsonDeserializer<>( NotificationMessage.class, objectMapper));
        return factory;
    }

    @Bean
    public ProducerFactory<String, NotificationMessage> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, NotificationMessage> factory = new DefaultKafkaProducerFactory<>(configProps);

        factory.setValueSerializer(new JsonSerializer<>(objectMapper));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationMessage> kafkaListenerContainerFactory(ConsumerFactory<String,
            NotificationMessage> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, NotificationMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic("ws-notification", 1, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic("pending-notification", 1, (short) 1);
    }

    @Bean
    public KafkaTemplate<String, NotificationMessage> kafkaTemplate(ProducerFactory<String, NotificationMessage> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "notification-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KTable<Long, List<NotificationMessage>> notificationPendingStreams(StreamsBuilder builder) {
        var stream = builder.stream("pending-notification", Consumed.with(Serdes.String(), JsonSerdes.notification()));

        KGroupedStream<Long, NotificationMessage> grouped = stream
                .selectKey((messageId, message) -> {
                    if (message != null) {
                        return message.getUserId();
                    }
                    return null;
                })
                .groupByKey(Grouped.with(Serdes.Long(), JsonSerdes.notification()));

        // Aggregate into List<NotificationMessage>
        return grouped.aggregate(
                ArrayList::new,
                (userId, newMessage, list) -> {
                    list.add(newMessage);
                    return list;
                },
                Materialized.<Long, List<NotificationMessage>, KeyValueStore<Bytes, byte[]>>as("pending-notification-store")
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(JsonSerdes.notificationList())
        );
    }
}
