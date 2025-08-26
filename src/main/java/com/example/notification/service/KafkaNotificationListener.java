package com.example.notification.service;

import com.example.notification.model.NotificationMessage;
import com.example.notification.model.NotificationState;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaNotificationListener {

    private final SimpMessagingTemplate simpMessagingTemplate;
    private final KafkaTemplate<String, NotificationMessage> kafkaTemplate;
    private final ConnectionManager connectionManager;

    @KafkaListener(topics = "ws-notification",
            groupId = "notification-#{T(java.util.UUID).randomUUID()}",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenNotification(ConsumerRecord<String, NotificationMessage> consumerRecord,
                                   Acknowledgment ack) {

        var message = consumerRecord.value();
        message.setLatency(System.currentTimeMillis() - message.getPublishedAt());
        ack.acknowledge();

        boolean isConnected = connectionManager.isConnected(message.getUserId());
        if (isConnected) {
            try {
                simpMessagingTemplate.convertAndSendToUser(String.valueOf(message.getUserId()), "/queue/notifications", message);
                message.setState(NotificationState.SENT);
                kafkaTemplate.send("notification-event-source", message.getMessageId(), message);
            } catch (Exception e) {
                log.error("Error while sending notification message", e);
            }
        }
    }
}
