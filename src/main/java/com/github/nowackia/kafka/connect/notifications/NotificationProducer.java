package com.github.nowackia.kafka.connect.notifications;

public interface NotificationProducer {

    /**
     * Sends Market Notification for MAR workflows
     * @param notification
     */
    void sendNotification(OrchestrationNotification notification);
    void close();
}
