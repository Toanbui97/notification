package com.example.notification;

import com.example.notification.model.NotificationMessage;
import com.example.notification.service.WsNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class NotificationPublisher {

    private final WsNotificationService wsNotificationService;

    @PostMapping("/api/v1/notifications")
    public ResponseEntity<NotificationMessage> publishNotification(@RequestBody NotificationMessage notification) {
        return ResponseEntity.ok().body(wsNotificationService.publishNotification(notification));
    }

}
