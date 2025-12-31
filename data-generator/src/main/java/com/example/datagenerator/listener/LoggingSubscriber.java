package com.example.datagenerator.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LoggingSubscriber {

    @PostgresEventHandler
    public void handleNotification(String payload) {
        log.info(">>> Received notification payload: {}", payload);
    }
}
