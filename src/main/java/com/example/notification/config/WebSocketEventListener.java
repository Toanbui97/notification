package com.example.notification.config;

import com.example.notification.model.NotificationState;
import com.example.notification.persistance.NotificationDocument;
import com.example.notification.service.ConnectionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketEventListener {

    private final ConnectionManager connectionManager;
    private final SimpMessagingTemplate simpMessagingTemplate;
    private final MongoTemplate mongoTemplate;

    @EventListener
    public void handleWebSocketConnectListener(SessionConnectedEvent event) {
        var headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        var sessionId = headerAccessor.getSessionId();

        var userId = Long.valueOf(event.getUser().getName());

        connectionManager.addConnection(userId, sessionId);
    }

    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        var user = event.getUser();
        if (user != null) {
            connectionManager.removeConnection(event.getSessionId());
        }
    }

    @EventListener
    public void handleWebSocketSubscribeListener(SessionSubscribeEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String userId = headerAccessor.getUser() != null ? headerAccessor.getUser().getName() : "0";

        var selectQuery = Query.query(Criteria.where("userId").is(Long.valueOf(userId)))
                .addCriteria(Criteria.where("state").is(NotificationState.PENDING));

        var notifications = mongoTemplate.find(selectQuery, NotificationDocument.class);

        var sentIds = notifications.stream().map(document -> {
                    try {
                        simpMessagingTemplate.convertAndSendToUser(userId, "/queue/notifications", document);
                        return document.getId();
                    } catch (Exception e) {
                        log.error("Error while sending notification message", e);
                    }
                    return null;
                }).filter(StringUtils::hasText)
                .toList();

        var query = Query.query(Criteria.where("id").in(sentIds));
        var update = Update.update("state", NotificationState.SENT);

        mongoTemplate.updateMulti(query, update, NotificationDocument.class);

//        ReadOnlyKeyValueStore<Long, Map<String, NotificationMessagePair>> store = factoryBean.getKafkaStreams()
//                .store(StoreQueryParameters.fromNameAndType("notification-store",
//                        QueryableStoreTypes.keyValueStore()));
//
//        Map<String, NotificationMessagePair> pendingNotifications = store.get(Long.valueOf(userId));
//
//        if (!CollectionUtils.isEmpty(pendingNotifications)) {
//
//            pendingNotifications.forEach((messageId, pair) -> {
//                var notification = pair.get();
//                simpMessagingTemplate.convertAndSendToUser(userId, "/queue/notifications", notification);
//
//                notification.setState(NotificationState.SENT);
//            });
//        }
    }
}
