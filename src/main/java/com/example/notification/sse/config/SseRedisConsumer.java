package com.example.notification.sse.config;

import com.example.notification.model.NotificationMessage;
import com.example.notification.sse.SseNotificationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SseRedisConsumer implements MessageListener {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void onMessage(Message msg, byte[] pattern) {
        var channel = new String(msg.getChannel(), StandardCharsets.UTF_8);
        var body = new String(msg.getBody(), StandardCharsets.UTF_8);

        NotificationMessage message;
        try {
            message = OBJECT_MAPPER.readValue(body, NotificationMessage.class);
            var latency = System.currentTimeMillis() - message.getPublishedAt();

            log.info("onMessage() - channel = {}, message = {}, receiveAt = {}, latency = {}ms.",
                    channel, message, OffsetDateTime.now(), latency);

        } catch (JsonProcessingException e) {
            log.error("onMessage() - Error parsing message: {}, channel = {}.", body, channel, e);
            return;
        }

        var sink = SseNotificationService.sinks.get(message.getUserId());
        if (sink != null) {
            sink.tryEmitNext(message);
        }
    }
}
