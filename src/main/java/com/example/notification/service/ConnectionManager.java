package com.example.notification.service;

import com.example.notification.model.ConnectionInfo;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConnectionManager {

    private final Map<Long, ConnectionInfo> connectionInfoMap = new ConcurrentHashMap<>();

    public void addConnection(Long userId, ConnectionInfo connectionInfo) {
        connectionInfoMap.computeIfAbsent(userId, id -> connectionInfo);
    }

    public void removeConnection(Long userId) {
        connectionInfoMap.remove(userId);
    }

    public boolean isConnected(Long userId) {
        return connectionInfoMap.containsKey(userId);
    }
}
