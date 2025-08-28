package com.example.notification.service;

import com.example.notification.model.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WsNotificationService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public NotificationMessage publishNotification(NotificationMessage notification) {
        log.info("publishNotification() - notification: {}, timestamps = {}.", notification, OffsetDateTime.now());
        notification.setPublishedAt(System.currentTimeMillis());
        notification.setMessageId(UUID.randomUUID().toString());

        kafkaTemplate.send("ws-notification", notification.getMessageId(), notification);
        return notification;
    }
}
