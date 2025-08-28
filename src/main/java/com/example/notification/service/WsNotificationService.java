package com.example.notification.service;

import com.example.notification.model.NotificationMessage;
import com.example.notification.model.NotificationState;
import com.example.notification.persistance.NotificationDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WsNotificationService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MongoTemplate mongoTemplate;

    public NotificationMessage publishNotification(NotificationMessage notification) {
        log.info("publishNotification() - notification: {}, timestamps = {}.", notification, OffsetDateTime.now());
        notification.setPublishedAt(System.currentTimeMillis());
        notification.setMessageId(UUID.randomUUID().toString());

        var notificationDoc = NotificationDocument.builder()
                .userId(notification.getUserId())
                .state(NotificationState.PENDING)
                .message(notification.getMessage())
                .publishedAt(Instant.now())
                .build();
        mongoTemplate.save(notificationDoc);

        notification.setMessageId(notificationDoc.getId());

        kafkaTemplate.send("notification-user-" + notification.getUserId(), notification.getMessageId(), notification);
        return notification;
    }
}
