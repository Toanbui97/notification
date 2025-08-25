package com.example.notification.service;

import com.example.notification.model.ConnectionInfo;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

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

    @Value("${spring.application.name:notification-service}")
    private String applicationName;

    @Getter
    private String instanceId;

    @PostConstruct
    public void init() {
        this.instanceId = applicationName + "-" + UUID.randomUUID();
    }

    public void addConnection(Long userId, String sessionId) {
        ConnectionInfo connectionInfo = ConnectionInfo.builder()
                .sessionId(sessionId)
                .userId(userId)
                .instanceId(instanceId).build();

        redisTemplate.opsForValue().set(
                SESSION_KEY + sessionId,
                connectionInfo
        );

        redisTemplate.opsForSet().add(USER_CONNECTIONS_KEY + userId, sessionId);
        redisTemplate.expire(USER_CONNECTIONS_KEY + userId, CONNECTION_TTL_MINUTES, TimeUnit.MINUTES);

        redisTemplate.opsForSet().add(INSTANCE_SESSIONS_KEY + instanceId, sessionId);
        redisTemplate.expire(INSTANCE_SESSIONS_KEY + instanceId, CONNECTION_TTL_MINUTES, TimeUnit.MINUTES);

        log.info("Connection added - User: {}, Session: {}, Instance: {}", userId, sessionId, instanceId);
    }

    public void removeConnection(String sessionId) {
        ConnectionInfo connectionInfo = (ConnectionInfo) redisTemplate.opsForValue().get(SESSION_KEY + sessionId);
        if (connectionInfo != null) {
            Long userId = connectionInfo.getUserId();

            redisTemplate.opsForSet().remove(USER_CONNECTIONS_KEY + userId, sessionId);
            redisTemplate.opsForSet().remove(INSTANCE_SESSIONS_KEY + instanceId, sessionId);
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
}
