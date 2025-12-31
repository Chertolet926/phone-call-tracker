package com.example.datagenerator.listener;

public interface PostgresEventSubscriber {
    void onNotification(String payload);
}
