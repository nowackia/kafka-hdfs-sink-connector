package com.github.nowackia.kafka.connect.notifications;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.github.nowackia.kafka.connect.notifications.OrchestrationNotificationDecoder.mapper;

public class MarketNotificationEncoder implements Serializer<OrchestrationNotification> {

    private static final Logger LOG = LoggerFactory.getLogger(MarketNotificationEncoder.class);

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    /**
     * Encoder for Market Notification Message
     */
    public byte[] serialize(String topic, OrchestrationNotification notification) {
        try {
            return mapper.writeValueAsString(notification).getBytes();
        } catch (JsonProcessingException e) {
            LOG.error("Exception in decoding Orchestration Notification message", e);
            return null;
        }

    }

    @Override
    public void close() {

    }
}