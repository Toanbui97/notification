package com.example.notification.ws.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Slf4j
@Configuration
@EnableWebSocket
@RequiredArgsConstructor
public class WebsocketConfiguration implements WebSocketConfigurer {

    private final NotificationWebsocketHandler notificationWebsocketHandler;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(notificationWebsocketHandler, "/ws/notifications/{userId}")
                .setAllowedOrigins("*");
    }
}
