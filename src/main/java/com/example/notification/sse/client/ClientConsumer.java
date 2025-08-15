package com.example.notification.sse.client;

import com.example.notification.model.NotificationMessage;
import com.example.notification.sse.SseNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;

@Slf4j
@RestController
@RequestMapping("/sse")
@RequiredArgsConstructor
public class ClientConsumer {

    private final SseNotificationService sseNotificationService;

    @GetMapping(value = "/notifications/{userId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<NotificationMessage> subscribeNotification(@PathVariable Long userId) {
        return sseNotificationService.subscribeNotification(userId);
    }
}
