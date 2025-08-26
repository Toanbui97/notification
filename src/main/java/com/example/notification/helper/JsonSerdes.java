package com.example.notification.helper;

import com.example.notification.model.NotificationMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@UtilityClass
public class JsonSerdes {
    public static Serde<NotificationMessage> notification() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(NotificationMessage.class));
    }

    public static Serde<NotificationMessagePair> notificationPair() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(NotificationMessagePair.class));
    }

    public static Serde<List<NotificationMessage>> notificationList() {
        ObjectMapper mapper = new ObjectMapper();
        return Serdes.serdeFrom(
                new JsonSerializer<>(mapper),
                new JsonDeserializer<>(new TypeReference<List<NotificationMessage>>() {}, mapper)
        );
    }

    public static Serde<Map<String, NotificationMessagePair>> notificationPairMap() {
        ObjectMapper mapper = new ObjectMapper();
        return Serdes.serdeFrom(
                new JsonSerializer<>(mapper),
                new JsonDeserializer<>(new TypeReference<Map<String, NotificationMessagePair>>() {}, mapper)
        );
    }
}