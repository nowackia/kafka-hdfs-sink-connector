package com.github.nowackia.kafka.connect.notifications;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import kafka.serializer.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrchestrationNotificationDecoder implements Decoder<OrchestrationNotification> {

    public static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new Jdk8Module());
    }

    private static final Logger LOG = LoggerFactory.getLogger(OrchestrationNotificationDecoder.class);

    /**
     *
     * Decodes OrchestrationNotification from Kafka Connect
     * @param bytes
     * @return
     */
    @Override
    public OrchestrationNotification fromBytes(byte[] bytes) {
        try {
            return mapper.readValue(bytes, ImmutableOrchestrationNotification.class);
        } catch (IOException e) {
            LOG.error("Exception in decoding Orchestration Notification message", e);
            return null;
        }
    }
}