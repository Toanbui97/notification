package com.example.notification.service;

import com.example.notification.model.NotificationMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class WsNotificationService {

    private final KafkaTemplate<String, NotificationMessage> kafkaTemplate;

    public NotificationMessage publishNotification(NotificationMessage notification) {
        log.info("publishNotification() - notification: {}, timestamps = {}.", notification, OffsetDateTime.now());
        notification.setPublishedAt(System.currentTimeMillis());
        kafkaTemplate.send("ws-notification", notification);
        return notification;
    }
}
