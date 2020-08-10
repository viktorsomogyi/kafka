/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
