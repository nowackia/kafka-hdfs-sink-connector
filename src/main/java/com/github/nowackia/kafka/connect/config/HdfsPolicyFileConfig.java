package com.github.nowackia.kafka.connect.config;

/**
 * Config for policy for hdfs sinking task
 */
public class HdfsPolicyFileConfig {

    private final String notificationTopic;
    private String location;

    public HdfsPolicyFileConfig(String notificationTopic, String location) {
        this.notificationTopic = notificationTopic;
        this.location = location;
    }

    public HdfsPolicyFileConfig(String notificationTopic) {
        this(notificationTopic, null);
    }

    public String getNotificationTopic() {
        return notificationTopic;
    }

    public String getLocation() {
        return location;
    }
}