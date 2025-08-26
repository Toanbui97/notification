package com.example.notification.helper;

import com.example.notification.model.NotificationMessage;
import com.example.notification.model.NotificationState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationMessagePair implements Serializable {

    private NotificationMessage sent;
    private NotificationMessage pending;

    public void add(NotificationMessage notificationMessage) {
        if (notificationMessage.getState() == NotificationState.SENT) {
            this.sent = notificationMessage;
        } else if (notificationMessage.getState() == NotificationState.PENDING) {
            this.pending = notificationMessage;
        }
    }

    public NotificationMessage get() {
        if (pending != null) {
            return pending;
        } else if (sent != null) {
            return sent;
        }
        return null;
    }

    @JsonIgnore
    public boolean isDone() {
        return this.sent != null && this.pending != null;
    }
}
