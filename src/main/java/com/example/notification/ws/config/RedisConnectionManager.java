package com.example.notification.ws.config;

import com.example.notification.model.ConnectionInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisConnectionManager {

    private static final String USER_CONNECTIONS_KEY = "user:connections:";
    private static final String SESSION_KEY = "session:";
    private static final String INSTANCE_SESSIONS_KEY = "instance:sessions:";
    private static final int CONNECTION_TTL_MINUTES = 30;

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${server.port:8080}")
    private String serverPort;

    @Value("${spring.application.name:notification-service}")
    private String applicationName;

    @Getter
    private String instanceId;

    @PostConstruct
    public void init() {
        this.instanceId = applicationName + "-" + serverPort + "-" + UUID.randomUUID();
    }

    public void addConnection(Long userId, String sessionId) {
        ConnectionInfo connectionInfo = ConnectionInfo.builder()
                .sessionId(sessionId)
                .userId(userId)
                .instanceId(instanceId).build();

        // Store connection info with TTL
        redisTemplate.opsForValue().set(
                SESSION_KEY + sessionId,
                connectionInfo
        );

        // Add session to user's connection set
        redisTemplate.opsForSet().add(USER_CONNECTIONS_KEY + userId, sessionId);
        redisTemplate.expire(USER_CONNECTIONS_KEY + userId, CONNECTION_TTL_MINUTES, TimeUnit.MINUTES);

        // Track sessions for this instance
        redisTemplate.opsForSet().add(INSTANCE_SESSIONS_KEY + instanceId, sessionId);
        redisTemplate.expire(INSTANCE_SESSIONS_KEY + instanceId, CONNECTION_TTL_MINUTES, TimeUnit.MINUTES);

        log.info("Connection added - User: {}, Session: {}, Instance: {}", userId, sessionId, instanceId);
    }

    public void removeConnection(String sessionId) {
        ConnectionInfo connectionInfo = (ConnectionInfo) redisTemplate.opsForValue().get(SESSION_KEY + sessionId);
        if (connectionInfo != null) {
            Long userId = connectionInfo.getUserId();

            // Remove from user's connection set
            redisTemplate.opsForSet().remove(USER_CONNECTIONS_KEY + userId, sessionId);

            // Remove from instance sessions
            redisTemplate.opsForSet().remove(INSTANCE_SESSIONS_KEY + instanceId, sessionId);

            // Remove session info
            redisTemplate.delete(SESSION_KEY + sessionId);

            log.info("Connection removed - User: {}, Session: {}", userId, sessionId);
        }
    }

    public Set<Object> getUserConnections(Long userId) {
        return redisTemplate.opsForSet().members(USER_CONNECTIONS_KEY + userId);
    }

    public ConnectionInfo getConnectionInfo(String sessionId) {
        return (ConnectionInfo) redisTemplate.opsForValue().get(SESSION_KEY + sessionId);
    }

    // Check if a user is connected to THIS instance
    public boolean isUserConnectedToThisInstance(Long userId) {
        Set<Object> userSessions = getUserConnections(userId);
        if (userSessions == null || userSessions.isEmpty()) {
            return false;
        }

        for (Object sessionId : userSessions) {
            ConnectionInfo info = getConnectionInfo(sessionId.toString());
            if (info != null && instanceId.equals(info.getInstanceId())) {
                return true;
            }
        }
        return false;
    }

    // Get all active sessions for this instance
    public Set<Object> getInstanceSessions() {
        return redisTemplate.opsForSet().members(INSTANCE_SESSIONS_KEY + instanceId);
    }

}
