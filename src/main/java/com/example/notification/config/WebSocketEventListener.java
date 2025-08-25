package com.example.notification.config;

import com.example.notification.model.NotificationMessage;
import com.example.notification.service.RedisConnectionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketEventListener {

    private final RedisConnectionManager connectionManager;
    private final StreamsBuilderFactoryBean factoryBean;
    private final KafkaTemplate<String, NotificationMessage> kafkaTemplate;

    @EventListener
    public void handleWebSocketConnectListener(SessionConnectEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String sessionId = headerAccessor.getSessionId();

        String userId = headerAccessor.getFirstNativeHeader("userId");

        if (userId != null && sessionId != null) {
            connectionManager.addConnection(Long.valueOf(userId), sessionId);
        }
    }

    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String sessionId = headerAccessor.getSessionId();

        if (sessionId != null) {
            connectionManager.removeConnection(sessionId);
        }
    }

    @EventListener
    public void handleWebSocketSubscribeListener(SessionSubscribeEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String userId = headerAccessor.getUser() != null ? headerAccessor.getUser().getName() : "unknown";
        String destination = headerAccessor.getDestination();
        log.info("User {} subscribed to: {}", userId, destination);
    }
}
