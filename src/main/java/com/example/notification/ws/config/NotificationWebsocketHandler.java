package com.example.notification.ws.config;

import com.example.notification.ws.WsNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationWebsocketHandler extends TextWebSocketHandler {

    private final WsNotificationService wsNotificationService;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {

        var userId = extractUserId(session);
        wsNotificationService.registerSession(userId, session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status){

        var userId = extractUserId(session);
        wsNotificationService.removeSession(userId, session);
    }

    private Long extractUserId(WebSocketSession session) {
        var path = Objects.requireNonNull(session.getUri()).getPath();
        return Long.valueOf(path.substring(path.lastIndexOf("/") + 1));
    }
}
