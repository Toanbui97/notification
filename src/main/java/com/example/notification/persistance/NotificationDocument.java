package com.example.notification.persistance;

import com.example.notification.model.NotificationState;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@Document(collection = "notifications")
@CompoundIndex(name = "messageId_idx", def = "{'messageId': 1}", unique = true)
public class NotificationDocument {
    @Id
    private String id;
    private Long userId;
    private String message;
    private NotificationState state;
    @Builder.Default
    private Instant publishedAt = Instant.now();
}
