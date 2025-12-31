package com.example.datagenerator.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LoggingSubscriber implements PostgresEventSubscriber {
    @Override
    public void onNotification(String payload) {
        log.debug(">>> Received notification payload: {}", payload);
    }
}
