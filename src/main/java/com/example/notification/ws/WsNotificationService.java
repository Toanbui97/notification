package com.example.notification.ws;

import com.example.notification.model.NotificationMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class WsNotificationService {

    private static final String WS_NOTIFICATION_CHANNEL = "WS_NOTIFICATION_CHANNEL";

    private final RedisTemplate<String, String> redisTemplate;

    public NotificationMessage publishNotification(NotificationMessage notification) {
        log.info("publishNotification() - notification: {}, timestamps = {}.", notification, OffsetDateTime.now());
        notification.setPublishedAt(System.currentTimeMillis());
        try {
            redisTemplate.convertAndSend(WS_NOTIFICATION_CHANNEL, new ObjectMapper().writeValueAsString(notification));
        } catch (JsonProcessingException e) {
            log.error("publishNotification() - error: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        return notification;
    }
}
