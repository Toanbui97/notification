package com.example.notification.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationMessage implements Serializable {
    private Long userId;
    private String messageId;
    private String message;
    private Long publishedAt;
    private String protocol;
    private Long latency;
    private NotificationState state = NotificationState.PENDING;
}
