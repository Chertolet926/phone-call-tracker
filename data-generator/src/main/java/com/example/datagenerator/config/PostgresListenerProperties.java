package com.example.datagenerator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import lombok.Data;

@Data
@Component
@ConfigurationProperties(prefix = "postgres.listener")
public class PostgresListenerProperties {
        private String channel = "user_events";         // имя канала
        private long restartDelayMs = 5000;             // задержка при перезапуске
        private int subscriberThreads = 4;              // количество потоков для подписчиков
        private int subscriberQueueCapacity = 100;      // размер очереди


        @Bean("subscriberExecutor")
        public TaskExecutor subscriberExecutor() {
                ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
                executor.setCorePoolSize(subscriberThreads);
                executor.setMaxPoolSize(subscriberThreads);
                executor.setQueueCapacity(subscriberQueueCapacity);
                executor.setThreadNamePrefix("pg-subscriber-");
                executor.initialize();
                return executor;
        }
}
