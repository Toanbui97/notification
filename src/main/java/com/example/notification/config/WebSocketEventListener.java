package com.example.notification.config;

import com.example.notification.helper.NotificationMessagePair;
import com.example.notification.model.ConnectionInfo;
import com.example.notification.model.NotificationMessage;
import com.example.notification.model.NotificationState;
import com.example.notification.service.ConnectionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketEventListener {

    private final ConnectionManager connectionManager;
    private final StreamsBuilderFactoryBean factoryBean;
    private final SimpMessagingTemplate simpMessagingTemplate;
    private final KafkaTemplate<String, NotificationMessage> kafkaTemplate;

    @EventListener
    public void handleWebSocketConnectListener(SessionConnectedEvent event) {
        var headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        var sessionId = headerAccessor.getSessionId();

        var userId = Long.valueOf(event.getUser().getName());

        var connectionInfo = ConnectionInfo.builder()
                .connectedAt(OffsetDateTime.now())
                .sessionId(sessionId)
                .userId(userId)
                .build();
        connectionManager.addConnection(userId, connectionInfo);
    }

    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        var user = event.getUser();
        if (user != null) {
            connectionManager.removeConnection(Long.valueOf(user.getName()));
        }
    }

    @EventListener
    public void handleWebSocketSubscribeListener(SessionSubscribeEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String userId = headerAccessor.getUser() != null ? headerAccessor.getUser().getName() : "unknown";

        ReadOnlyKeyValueStore<Long, Map<String, NotificationMessagePair>> store = factoryBean.getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType("notification-store",
                        QueryableStoreTypes.keyValueStore()));

        Map<String, NotificationMessagePair> pendingNotifications = store.get(Long.valueOf(userId));

        if (!CollectionUtils.isEmpty(pendingNotifications)) {

            pendingNotifications.forEach((messageId, pair) -> {
                var notification = pair.get();
                simpMessagingTemplate.convertAndSendToUser(userId, "/queue/notifications", notification);

                notification.setState(NotificationState.SENT);
                kafkaTemplate.send("notification-event-source", messageId, notification);
            });
        }
    }
}
