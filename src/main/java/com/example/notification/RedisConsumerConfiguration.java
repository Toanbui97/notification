package com.example.notification;

import com.example.notification.sse.config.SseRedisConsumer;
import com.example.notification.ws.config.WsRedisConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
@RequiredArgsConstructor
public class RedisConsumerConfiguration {

    private final SseRedisConsumer sseRedisConsumer;
    private final WsRedisConsumer wsRedisConsumer;

    @Bean
    public RedisMessageListenerContainer redisContainer(LettuceConnectionFactory lettuceConnectionFactory) {
        var container = new RedisMessageListenerContainer();
        container.setConnectionFactory(lettuceConnectionFactory);

        // Subscribe to one or more channels
        container.addMessageListener(sseRedisConsumer, new ChannelTopic("SSE_NOTIFICATION_CHANNEL"));
        container.addMessageListener(wsRedisConsumer, new ChannelTopic("WS_NOTIFICATION_CHANNEL"));

        return container;
    }
}
