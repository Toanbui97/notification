package com.example.notification.controller;

import com.example.notification.helper.NotificationMessagePair;
import com.example.notification.model.ClientAckMessage;
import com.example.notification.model.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Map;

@Slf4j
@Controller
@RequiredArgsConstructor
public class WebSocketController {

    private final StreamsBuilderFactoryBean factoryBean;
    private final KafkaTemplate<String, NotificationMessage> kafkaTemplate;

    @MessageMapping("/notifications/ack")
    public void clientAck(@Payload ClientAckMessage ackMessage) {
        log.info("clientAck() - received message: {}", ackMessage.toString());

        ReadOnlyKeyValueStore<Long, Map<String, NotificationMessagePair>> store = factoryBean.getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType("notification-store",
                        QueryableStoreTypes.keyValueStore()));

        Map<String, NotificationMessagePair> messages = store.get(ackMessage.getUserId());
//        if (messages != null) {
//            messages.stream().filter(e -> e.getMessageId().equals(ackMessage.getMessageId()))
//                    .findFirst()
//                    .ifPresent(e -> kafkaTemplate.send("pending-notification", ackMessage.getMessageId(), null));
//        }
        log.info("clientAck() - pending messages: {}", messages);
        log.info("clientAck() - notification commited. messageId = {}.", ackMessage.getMessageId());
    }
}
