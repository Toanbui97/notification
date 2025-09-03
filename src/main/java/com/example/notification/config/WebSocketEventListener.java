package com.example.notification.config;

import com.example.notification.model.NotificationMessage;
import com.example.notification.model.NotificationState;
import com.example.notification.persistance.NotificationDocument;
import com.example.notification.service.ConnectionManager;
import com.example.notification.service.KafkaNotificationListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.config.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MappingJacksonParameterizedConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketEventListener {

    private final ConnectionManager connectionManager;
    private final SimpMessagingTemplate simpMessagingTemplate;
    private final MongoTemplate mongoTemplate;
    private final KafkaAdmin kafkaAdmin;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory;
    private final KafkaNotificationListener kafkaNotificationListener;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Map<Long, String> kafkaTopicConsumerMap = new ConcurrentHashMap<>();

    @Value("${application.instance-name}")
    private String instanceName;

    @EventListener
    public void handleWebSocketConnectListener(SessionConnectedEvent event) {
        var headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        var sessionId = headerAccessor.getSessionId();

        var userId = Long.valueOf(event.getUser().getName());

        createTopicAndSubscribeForUser(userId);
        connectionManager.addConnection(userId, sessionId);
    }

    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        var user = event.getUser();
        if (user != null) {
            connectionManager.removeConnection(event.getSessionId());
            var userId = Long.valueOf(user.getName());
            unregisterDynamicListenerForUser(userId).thenRun(() -> {
                    if (!connectionManager.isUserConnected(Long.valueOf(user.getName()))) {
                        CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS).execute(() -> {
                            deleteTopic("notification-user-" + user.getName());
                        });
                    }
            });
        }
    }

    @EventListener
    public void handleWebSocketSubscribeListener(SessionSubscribeEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        Long userId = headerAccessor.getUser() != null ? Long.parseLong(headerAccessor.getUser().getName()) : 0;


        var selectQuery = Query.query(Criteria.where("userId").is(userId))
                .addCriteria(Criteria.where("state").is(NotificationState.PENDING));

        var notifications = mongoTemplate.find(selectQuery, NotificationDocument.class);
        notifications.forEach(document -> {
            var message = NotificationMessage.builder()
                    .messageId(document.getId())
                    .message(document.getMessage())
                    .publishedAt(document.getPublishedAt().toEpochMilli())
                    .userId(document.getUserId())
                    .state(document.getState())
                    .build();

            simpMessagingTemplate.convertAndSendToUser(String.valueOf(message.getUserId()), "/queue/notifications", message);
        });

        var updateQuery = Update.update("state", NotificationState.SENT);
        mongoTemplate.updateMulti(selectQuery, updateQuery, NotificationDocument.class);
    }

    private void createTopicAndSubscribeForUser(Long userId) {
        String topicName = "notification-user-" + userId;

        try {
            createTopicForUser(topicName);
            createDynamicListenerForUser(userId, "notification-user-" + userId);
        } catch (Exception e) {
            log.error("Failed to create topic and listener for user {}", userId, e);
        }
    }

    private void createTopicForUser(String topicName) {
        try {
            if (!isTopicExists(topicName)) {
                kafkaAdmin.createOrModifyTopics(
                        TopicBuilder.name(topicName)
                                .partitions(1)
                                .replicas(1)
//                                .config("retention.ms", "604800000") // 7 days
                                .config("cleanup.policy", "delete")
                                .build());
                log.info("createTopicForUser() - Topic create successfully. Topic: {}", topicName);
            }
        } catch (Exception e) {
            log.error("createTopicForUser() - Topic creation failed. Topic: {}", topicName, e);
        }
    }

    private boolean isTopicExists(String topicName) {
        try (var adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return adminClient.listTopics()
                    .names()
                    .get()
                    .contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            log.error("isTopicExists() - Topic not found. Topic: {}", topicName, e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void createDynamicListenerForUser(Long userId, String topicName) {

        if (connectionManager.isUserConnectedToThisInstance(userId)) {
            return;
        }

        String listenerId = "listener-user-" + userId;
        String groupId = "user-notification-groupId-" + userId + "-" + UUID.randomUUID();

        MethodKafkaListenerEndpoint<String, NotificationMessage> endpoint =
                new MethodKafkaListenerEndpoint<>();

        endpoint.setId(listenerId);
        endpoint.setTopics(topicName);
        endpoint.setGroupId(groupId);
        endpoint.setAutoStartup(true);

        endpoint.setBean(kafkaNotificationListener);
        endpoint.setMethod(getListenerMethod());

        endpoint.setMessagingConverter(new MappingJacksonParameterizedConverter());
        endpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());

        kafkaListenerEndpointRegistry.registerListenerContainer(
                endpoint,
                kafkaListenerContainerFactory,
                true
        );

        kafkaTopicConsumerMap.put(userId, groupId);
        log.info("createDynamicListenerForUser() - Endpoint created. Topic: {}", topicName);
    }

    private CompletableFuture<Void> unregisterDynamicListenerForUser(Long userId) {
        if (connectionManager.isUserConnectedToThisInstance(userId)) {
            return new CompletableFuture<>();
        }

        return CompletableFuture.runAsync(() -> {

            String listenerId = "listener-user-" + userId;
            MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);

            if (container != null) {
                try {
                    container.stop();

                    int maxWait = 50;
                    int waited = 0;
                    while (container.isRunning() && waited < maxWait) {
                        Thread.sleep(100);
                        waited++;
                    }

                    if (container.isRunning()) {
                        log.warn("Container for user {} did not stop gracefully within timeout", userId);
                    }

                    kafkaListenerEndpointRegistry.unregisterListenerContainer(listenerId);

                    deleteConsumerGroup(kafkaTopicConsumerMap.get(userId));
                    log.info("Successfully unregistered listener for user: {}", userId);
                    kafkaTopicConsumerMap.remove(userId);

                } catch (Exception e) {
                    log.error("Error unregistering listener for user {}: {}", userId, e.getMessage());
                }
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    private void deleteConsumerGroup(String groupId) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DeleteConsumerGroupsResult result = adminClient.deleteConsumerGroups(Collections.singleton(groupId));
            result.all().get(30, TimeUnit.SECONDS);
            log.info("deleteConsumerGroup() - Consumer group deleted successfully: {}", groupId);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e.getCause() instanceof GroupNotEmptyException) {
                log.warn("deleteConsumerGroup() - Consumer group not empty: {}", groupId);
            } else if (e.getCause() instanceof GroupIdNotFoundException) {
                log.info("deleteConsumerGroup() - Consumer group does not exist: {}", groupId);
            } else {
                log.error("deleteConsumerGroup() - Failed to delete consumer group: {}", groupId, e);
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Method getListenerMethod() {
        try {
            return kafkaNotificationListener.getClass().getDeclaredMethod("listenNotification",
                    ConsumerRecord.class, Acknowledgment.class);
        } catch (NoSuchMethodException e) {
            log.error("getListenerMethod() - No such method found", e);
            throw new RuntimeException("Dynamic listener method not found", e);
        }
    }

    private void deleteTopic(String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
            result.all().get(); // block until complete
            log.info("deleteTopic() - Topic deleted successfully. Topic: {}", topicName);
        } catch (InterruptedException | ExecutionException e) {
            log.error("deleteTopic() - Topic deletion failed. Topic: {}", topicName, e);
            Thread.currentThread().interrupt();
        }
    }
}
