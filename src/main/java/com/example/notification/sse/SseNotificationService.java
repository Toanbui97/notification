package com.example.notification.sse;

import com.example.notification.model.NotificationMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class SseNotificationService {

    public static final Map<Long, Sinks.Many<NotificationMessage>> sinks = new ConcurrentHashMap<>();
    private static final String SSE_NOTIFICATION_CHANNEL = "SSE_NOTIFICATION_CHANNEL";

    private final RedisTemplate<String, Object> redisTemplate;

    public Flux<NotificationMessage> subscribeNotification(Long userId) {
        log.info("subscribeNotification() - userId: {}", userId);

        var sink = sinks.computeIfAbsent(userId, id ->
                    Sinks.many().multicast().onBackpressureBuffer());

        return sink.asFlux()
                .doFinally(signal -> {
                    log.info("subscribeNotification() - signal : {}", signal);
                    removeSink(userId);
                });
    }

    public void publishNotification(NotificationMessage notification) {
        log.info("publishNotification() - notification: {}, timestamps = {}.", notification, OffsetDateTime.now());
        notification.setPublishedAt(System.currentTimeMillis());
        try {
            redisTemplate.convertAndSend(SSE_NOTIFICATION_CHANNEL, new ObjectMapper().writeValueAsString(notification));
        } catch (JsonProcessingException e) {
            log.error("publishNotification() - error: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }


    public static void removeSink(Long userId) {
        var sink = sinks.remove(userId);
        if (sink != null) {
            sink.tryEmitComplete();
        }

        log.info("removeSink() - userId = {}", userId);
        log.info("removeSink() - current sinks = {}", sinks.keySet());
    }
}
