package com.example.datagenerator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import lombok.Data;

@Data
@Component
@ConfigurationProperties(prefix = "postgress.listener")
public class PostgresListenerProperties {
        private String channel = "user_events";  // имя канала
        private long restartDelayMs = 5000;         // задержка при перезапуске
        private int subscriberThreads = 4;         // количество потоков для подписчиков
        private int subscriberQueueCapacity = 100; // размер очереди
}
