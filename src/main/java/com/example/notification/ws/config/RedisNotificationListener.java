package com.example.notification.ws.config;

import com.example.notification.model.NotificationMessage;
import com.example.notification.ws.WsNotificationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisNotificationListener implements MessageListener {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final SimpMessagingTemplate messagingTemplate;
    private final RedisConnectionManager connectionManager;

    @Override
    public void onMessage(Message msg, byte[] pattern) {
        var channel = new String(msg.getChannel(), StandardCharsets.UTF_8);
        var body = new String(msg.getBody(), StandardCharsets.UTF_8);

        NotificationMessage message;
        try {
            message = OBJECT_MAPPER.readValue(body, NotificationMessage.class);
            var latency = System.currentTimeMillis() - message.getPublishedAt();
            message.setLatency(latency);
            log.info("onMessage() - channel = {}, message = {}, receiveAt = {}, latency = {}ms.",
                    channel, message, OffsetDateTime.now(), latency);

        } catch (JsonProcessingException e) {
            log.error("onMessage() - Error parsing message: {}, channel = {}.", body, channel, e);
            return;
        }

        if (connectionManager.isUserConnectedToThisInstance(message.getUserId())) {
            messagingTemplate.convertAndSendToUser(
                    String.valueOf(message.getUserId()),
                    "/queue/notifications",
                    message
            );
        }
    }
}
