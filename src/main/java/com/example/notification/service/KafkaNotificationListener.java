package com.example.notification.service;

import com.example.notification.helper.JsonSerdes;
import com.example.notification.model.NotificationMessage;
import com.example.notification.model.NotificationState;
import com.example.notification.persistance.NotificationDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaNotificationListener {

    private final SimpMessagingTemplate simpMessagingTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ConnectionManager connectionManager;
    private final MongoTemplate mongoTemplate;


    @KafkaListener(topics = "ws-notification",
            groupId = "notification-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenNotification(ConsumerRecord<String, NotificationMessage> consumerRecord,
                                   Acknowledgment ack) {

        var message = consumerRecord.value();
        message.setLatency(System.currentTimeMillis() - message.getPublishedAt());

        var isUserConnected = connectionManager.isUserConnected(message.getUserId());

        var document = mongoTemplate.save(NotificationDocument.builder()
                .userId(message.getUserId())
                .state(NotificationState.PENDING)
                .message(message.getMessage())
                .publishedAt(Instant.now())
                .build());

        message.setMessageId(document.getId());

        if (isUserConnected) {
            kafkaTemplate.send("broadcast-notification", message);
        }

        ack.acknowledge();
    }

    @Bean
    public KStream<String, NotificationMessage> processBroadcastNotification(StreamsBuilder builder) {
        KStream<String, NotificationMessage> stream = builder.stream("broadcast-notification",
                Consumed.with(Serdes.String(), JsonSerdes.notification()));

        stream.peek((key, notificationMessage) -> {
            var userId = notificationMessage.getUserId();
            if (connectionManager.isUserConnectedToThisInstance(userId)) {
                try {
                    simpMessagingTemplate.convertAndSendToUser(String.valueOf(userId), "/queue/notifications", notificationMessage);
                    var query = Query.query(Criteria.where("id").is(notificationMessage.getMessageId()));
                    var update = Update.update("state", NotificationState.SENT);
                    mongoTemplate.updateFirst(query, update, NotificationDocument.class);
                } catch (Exception e) {
                    log.error("processBroadcastNotification() - Error while send websocket notification.", e);
                }
            }
        });

        return stream;
    }
}
