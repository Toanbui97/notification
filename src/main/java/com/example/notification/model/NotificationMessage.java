package com.example.notification.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class NotificationMessage implements Serializable {
    private Long userId;
    private String message;
    private Long publishedAt;
    private String protocol;
}
