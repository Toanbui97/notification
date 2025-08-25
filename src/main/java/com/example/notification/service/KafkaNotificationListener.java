package com.example.notification.service;

import com.example.notification.model.ClientAckMessage;
import com.example.notification.model.NotificationMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaNotificationListener {

    private final ObjectMapper objectMapper;
    private final SimpMessagingTemplate simpMessagingTemplate;
    private final KafkaTemplate<String, NotificationMessage> kafkaTemplate;

    @KafkaListener(topics = "ws-notification",
            groupId = "notification-#{T(java.util.UUID).randomUUID()}",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenNotification(ConsumerRecord<String, NotificationMessage> consumerRecord,
                                   Acknowledgment ack) {

        var message = consumerRecord.value();

        message.setLatency(System.currentTimeMillis() - message.getPublishedAt());
        message.setMessageId(UUID.randomUUID().toString());

        simpMessagingTemplate.convertAndSendToUser(String.valueOf(message.getUserId()), "/queue/notifications", message);
        kafkaTemplate.send("pending-notification", message.getMessageId(), message);
    }
}
