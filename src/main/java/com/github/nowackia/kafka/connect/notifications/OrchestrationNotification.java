package com.github.nowackia.kafka.connect.notifications;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = ImmutableOrchestrationNotification.class)
@JsonDeserialize(as = ImmutableOrchestrationNotification.class)
public interface OrchestrationNotification {

    long timestamp();

    String inputPath();

    String cobDate();

    String sourceSystem();

    String topic();

}