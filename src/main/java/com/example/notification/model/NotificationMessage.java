package com.example.notification.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationMessage implements Serializable {
    private Long userId;
    private String messageId;
    private String message;
    private Long publishedAt;
    @Builder.Default
    private NotificationState state = NotificationState.PENDING;
}
