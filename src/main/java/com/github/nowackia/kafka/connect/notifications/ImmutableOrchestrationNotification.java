package com.github.nowackia.kafka.connect.notifications;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@Immutable
public final class ImmutableOrchestrationNotification implements OrchestrationNotification {
    private final long timestamp;
    private final String inputPath;
    private final String cobDate;
    private final String sourceSystem;
    private final String topic;

    private ImmutableOrchestrationNotification(
            long timestamp,
            String inputPath,
            String cobDate,
            String sourceSystem,
            String topic) {
        this.timestamp = timestamp;
        this.inputPath = inputPath;
        this.cobDate = cobDate;
        this.sourceSystem = sourceSystem;
        this.topic = topic;
    }

    /**
     * @return The value of the {@code timestamp} attribute
     */
    @JsonProperty("timestamp")
    @Override
    public long timestamp() {
        return timestamp;
    }

    /**
     * @return The value of the {@code inputPath} attribute
     */
    @JsonProperty("inputPath")
    @Override
    public String inputPath() {
        return inputPath;
    }

    /**
     * @return The value of the {@code cobDate} attribute
     */
    @JsonProperty("cobDate")
    @Override
    public String cobDate() {
        return cobDate;
    }

    /**
     * @return The value of the {@code sourceSystem} attribute
     */
    @JsonProperty("sourceSystem")
    @Override
    public String sourceSystem() {
        return sourceSystem;
    }

    /**
     * @return The value of the {@code topic} attribute
     */
    @JsonProperty("topic")
    @Override
    public String topic() {
        return topic;
    }

    public final ImmutableOrchestrationNotification withTimestamp(long value) {
        if (this.timestamp == value) return this;
        return new ImmutableOrchestrationNotification(
                value,
                this.inputPath,
                this.cobDate,
                this.sourceSystem,
                this.topic);
    }

    public final ImmutableOrchestrationNotification withInputPath(String value) {
        if (this.inputPath.equals(value)) return this;
        String newValue = Objects.requireNonNull(value, "inputPath");
        return new ImmutableOrchestrationNotification(
                this.timestamp,
                newValue,
                this.cobDate,
                this.sourceSystem,
                this.topic);
    }

    public final ImmutableOrchestrationNotification withCobDate(String value) {
        if (this.cobDate.equals(value)) return this;
        String newValue = Objects.requireNonNull(value, "cobDate");
        return new ImmutableOrchestrationNotification(
                this.timestamp,
                this.inputPath,
                newValue,
                this.sourceSystem,
                this.topic);
    }

    public final ImmutableOrchestrationNotification withSourceSystem(String value) {
        if (this.sourceSystem.equals(value)) return this;
        String newValue = Objects.requireNonNull(value, "sourceSystem");
        return new ImmutableOrchestrationNotification(
                this.timestamp,
                this.inputPath,
                this.cobDate,
                newValue,
                this.topic);
    }

    public final ImmutableOrchestrationNotification withTopic(String value) {
        if (this.topic.equals(value)) return this;
        String newValue = Objects.requireNonNull(value, "topic");
        return new ImmutableOrchestrationNotification(
                this.timestamp,
                this.inputPath,
                this.cobDate,
                this.sourceSystem,
                newValue);
    }

    @Override
    public boolean equals(@Nullable Object another) {
        if (this == another) return true;
        return another instanceof ImmutableOrchestrationNotification
                && equalTo((ImmutableOrchestrationNotification) another);
    }

    private boolean equalTo(ImmutableOrchestrationNotification another) {
        return timestamp == another.timestamp
                && inputPath.equals(another.inputPath)
                && cobDate.equals(another.cobDate)
                && sourceSystem.equals(another.sourceSystem)
                && topic.equals(another.topic);
    }

    @Override
    public int hashCode() {
        int h = 5381;
        h += (h << 5) + Long.hashCode(timestamp);
        h += (h << 5) + inputPath.hashCode();
        h += (h << 5) + cobDate.hashCode();
        h += (h << 5) + sourceSystem.hashCode();
        h += (h << 5) + topic.hashCode();
        return h;
    }

    @Override
    public String toString() {
        return "OrchestrationNotification{"
                + "timestamp=" + timestamp
                + ", inputPath=" + inputPath
                + ", cobDate=" + cobDate
                + ", sourceSystem=" + sourceSystem
                + ", topic=" + topic
                + "}";
    }

    @Deprecated
    @JsonDeserialize
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
    static final class Json implements OrchestrationNotification {
        long timestamp;
        boolean timestampIsSet;
        @Nullable String inputPath;
        @Nullable String cobDate;
        @Nullable String sourceSystem;
        @Nullable String topic;
        @JsonProperty("timestamp")
        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            this.timestampIsSet = true;
        }
        @JsonProperty("inputPath")
        public void setInputPath(String inputPath) {
            this.inputPath = inputPath;
        }
        @JsonProperty("cobDate")
        public void setCobDate(String cobDate) {
            this.cobDate = cobDate;
        }
        @JsonProperty("sourceSystem")
        public void setSourceSystem(String sourceSystem) {
            this.sourceSystem = sourceSystem;
        }
        @JsonProperty("topic")
        public void setTopic(String topic) {
            this.topic = topic;
        }
        @Override
        public long timestamp() { throw new UnsupportedOperationException(); }
        @Override
        public String inputPath() { throw new UnsupportedOperationException(); }
        @Override
        public String cobDate() { throw new UnsupportedOperationException(); }
        @Override
        public String sourceSystem() { throw new UnsupportedOperationException(); }
        @Override
        public String topic() { throw new UnsupportedOperationException(); }
    }

    /**
     * @param json A JSON-bindable data structure
     * @return An immutable value type
     * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
     */
    @Deprecated
    @JsonCreator
    static ImmutableOrchestrationNotification fromJson(Json json) {
        ImmutableOrchestrationNotification.Builder builder = ((ImmutableOrchestrationNotification.Builder) ImmutableOrchestrationNotification.builder());
        if (json.timestampIsSet) {
            builder.timestamp(json.timestamp);
        }
        if (json.inputPath != null) {
            builder.inputPath(json.inputPath);
        }
        if (json.cobDate != null) {
            builder.cobDate(json.cobDate);
        }
        if (json.sourceSystem != null) {
            builder.sourceSystem(json.sourceSystem);
        }
        if (json.topic != null) {
            builder.topic(json.topic);
        }
        return builder.build();
    }

    /**
     * Creates an immutable copy of a {@link OrchestrationNotification} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable OrchestrationNotification instance
     */
    public static ImmutableOrchestrationNotification copyOf(OrchestrationNotification instance) {
        if (instance instanceof ImmutableOrchestrationNotification) {
            return (ImmutableOrchestrationNotification) instance;
        }
        return ((ImmutableOrchestrationNotification.Builder) ImmutableOrchestrationNotification.builder())
                .timestamp(instance.timestamp())
                .inputPath(instance.inputPath())
                .cobDate(instance.cobDate())
                .sourceSystem(instance.sourceSystem())
                .topic(instance.topic())
                .build();
    }

    public static TimestampBuildStage builder() {
        return new ImmutableOrchestrationNotification.Builder();
    }

    @NotThreadSafe
    public static final class Builder implements TimestampBuildStage, InputPathBuildStage, CobDateBuildStage, SourceSystemBuildStage, TopicBuildStage, BuildFinal {
        private static final long INIT_BIT_TIMESTAMP = 0x1L;
        private static final long INIT_BIT_INPUT_PATH = 0x2L;
        private static final long INIT_BIT_COB_DATE = 0x4L;
        private static final long INIT_BIT_SOURCE_SYSTEM = 0x8L;
        private static final long INIT_BIT_TOPIC = 0x10L;
        private long initBits = 0x7fL;

        private long timestamp;
        private @Nullable String inputPath;
        private @Nullable String cobDate;
        private @Nullable String sourceSystem;
        private @Nullable String topic;

        private Builder() {
        }

        @JsonProperty("timestamp")
        public final Builder timestamp(long timestamp) {
            checkNotIsSet(timestampIsSet(), "timestamp");
            this.timestamp = timestamp;
            initBits &= ~INIT_BIT_TIMESTAMP;
            return this;
        }

        @JsonProperty("inputPath")
        public final Builder inputPath(String inputPath) {
            checkNotIsSet(inputPathIsSet(), "inputPath");
            this.inputPath = Objects.requireNonNull(inputPath, "inputPath");
            initBits &= ~INIT_BIT_INPUT_PATH;
            return this;
        }

        @JsonProperty("cobDate")
        public final Builder cobDate(String cobDate) {
            checkNotIsSet(cobDateIsSet(), "cobDate");
            this.cobDate = Objects.requireNonNull(cobDate, "cobDate");
            initBits &= ~INIT_BIT_COB_DATE;
            return this;
        }

        @JsonProperty("sourceSystem")
        public final Builder sourceSystem(String sourceSystem) {
            checkNotIsSet(sourceSystemIsSet(), "sourceSystem");
            this.sourceSystem = Objects.requireNonNull(sourceSystem, "sourceSystem");
            initBits &= ~INIT_BIT_SOURCE_SYSTEM;
            return this;
        }

        @JsonProperty("topic")
        public final Builder topic(String topic) {
            checkNotIsSet(topicIsSet(), "topic");
            this.topic = Objects.requireNonNull(topic, "topic");
            initBits &= ~INIT_BIT_TOPIC;
            return this;
        }

        public ImmutableOrchestrationNotification build() {
            checkRequiredAttributes();
            return new ImmutableOrchestrationNotification(timestamp, inputPath, cobDate, sourceSystem, topic);
        }

        private boolean timestampIsSet() {
            return (initBits & INIT_BIT_TIMESTAMP) == 0;
        }

        private boolean inputPathIsSet() {
            return (initBits & INIT_BIT_INPUT_PATH) == 0;
        }

        private boolean cobDateIsSet() {
            return (initBits & INIT_BIT_COB_DATE) == 0;
        }

        private boolean sourceSystemIsSet() {
            return (initBits & INIT_BIT_SOURCE_SYSTEM) == 0;
        }

        private boolean topicIsSet() {
            return (initBits & INIT_BIT_TOPIC) == 0;
        }

        private void checkNotIsSet(boolean isSet, String name) {
            if (isSet) throw new IllegalStateException("Builder of OrchestrationNotification is strict, attribute is already set: ".concat(name));
        }

        private void checkRequiredAttributes() throws IllegalStateException {
            if (initBits != 0) {
                throw new IllegalStateException(formatRequiredAttributesMessage());
            }
        }

        private String formatRequiredAttributesMessage() {
            List<String> attributes = new ArrayList<String>();
            if (!timestampIsSet()) attributes.add("timestamp");
            if (!inputPathIsSet()) attributes.add("inputPath");
            if (!cobDateIsSet()) attributes.add("cobDate");
            if (!sourceSystemIsSet()) attributes.add("sourceSystem");
            if (!topicIsSet()) attributes.add("topic");
            return "Cannot build OrchestrationNotification, some of required attributes are not set " + attributes;
        }
    }

    public interface TimestampBuildStage {
        InputPathBuildStage timestamp(long timestamp);
    }

    public interface InputPathBuildStage {
        CobDateBuildStage inputPath(String inputPath);
    }

    public interface CobDateBuildStage {
        SourceSystemBuildStage cobDate(String cobDate);
    }

    public interface SourceSystemBuildStage {
        TopicBuildStage sourceSystem(String sourceSystem);
    }

    public interface TopicBuildStage {
        BuildFinal topic(String topic);
    }

    public interface BuildFinal {
        ImmutableOrchestrationNotification build();
    }
}
