package org.apache.kafka.common.security.audit.types;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.audit.AuditEventType;

import java.util.Optional;

public class TopicUpdatedEvent implements AuditEventType {

    private final String topicName;
    private final Integer partitions;
    private final Integer replicationFactor;
    private final Errors error;

    public static TopicUpdatedEvent updatedPartitionCount(String topicName, Integer partitions, Errors error) {
        return new TopicUpdatedEvent(topicName, partitions, null, error);
    }

    public static TopicUpdatedEvent updatedReplicationFactor(String topicName, Integer replicationFactor, Errors error) {
        return new TopicUpdatedEvent(topicName, null, replicationFactor, error);
    }

    private TopicUpdatedEvent(String topicName, Integer partitions, Integer replicationFactor, Errors error) {
        this.topicName = topicName;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.error = error;
    }

    public String topicName() {
        return topicName;
    }

    public Optional<Integer> partitions() {
        return Optional.ofNullable(partitions);
    }

    public Optional<Integer> replicationFactor() {
        return Optional.ofNullable(replicationFactor);
    }

    @Override
    public String toString() {
        return "TopicCreatedEvent{" +
            "topicName='" + topicName + '\'' +
            ", partitions=" + partitions +
            ", replicationFactor=" + replicationFactor +
            ", error=" + error +
            '}';
    }
}
