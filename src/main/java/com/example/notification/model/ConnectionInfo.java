package com.example.notification.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionInfo {
    private String sessionId;
    private Long userId;
    private String instanceId;
    private OffsetDateTime connectedAt;
    private OffsetDateTime lastHeartbeat;
}
