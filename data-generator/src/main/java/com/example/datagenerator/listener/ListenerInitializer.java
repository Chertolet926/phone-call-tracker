package com.example.datagenerator.listener;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ListenerInitializer implements CommandLineRunner {
    private final PostgresNotificationListener listener;
    private final LoggingSubscriber subscriber;

    public ListenerInitializer(PostgresNotificationListener listener, LoggingSubscriber subscriber) {
        this.listener = listener;
        this.subscriber = subscriber;
    }

    @Override public void run(String... args) {
        listener.subscribe(subscriber);
    }
}