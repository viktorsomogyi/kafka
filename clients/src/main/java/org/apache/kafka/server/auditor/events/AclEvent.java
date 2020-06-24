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

package org.apache.kafka.server.auditor.events;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.AuditInfo;
import org.apache.kafka.server.authorizer.AclDeleteResult;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@InterfaceStability.Evolving
public class AclEvent<T, R> extends AuditEvent {

    public enum EventType {
        CREATE, DELETE, DESCRIBE;
    }

    private final AuditInfo clusterAuditInfo;
    private final Set<T> auditedEntities;
    private final EventType eventType;
    private final Map<R, Integer> operationResults;

    public static AclEvent<AclBinding, AclBinding> aclCreateEvent(Set<AclBinding> auditedEntities,
                                                                  AuditInfo clusterAuditInfo) {
        return new AclEvent<>(auditedEntities, clusterAuditInfo, Collections.emptyMap(), EventType.CREATE);
    }

    public static AclEvent<AclBinding, AclBinding> aclCreateEvent(Set<AclBinding> auditedEntities,
                                                                  AuditInfo clusterAuditInfo,
                                                                  Map<AclBinding, Integer> results) {
        return new AclEvent<>(auditedEntities, clusterAuditInfo, results, EventType.CREATE);
    }

    public static AclEvent<AclBindingFilter, AclDeleteResult> aclDeleteEvent(Set<AclBindingFilter> auditedEntities,
                                                                             AuditInfo clusterAuditInfo) {
        return new AclEvent<>(auditedEntities, clusterAuditInfo, Collections.emptyMap(), EventType.DELETE);
    }

    public static AclEvent<AclBindingFilter, AclDeleteResult> aclDeleteEvent(Set<AclBindingFilter> auditedEntities,
                                                                             AuditInfo clusterAuditInfo,
                                                                             Map<AclDeleteResult, Integer> results) {
        return new AclEvent<>(auditedEntities, clusterAuditInfo, results, EventType.DELETE);
    }

    public static AclEvent<AclBindingFilter, AclBinding> aclDescribeEvent(Set<AclBindingFilter> auditedEntities,
                                                                          AuditInfo clusterAuditInfo,
                                                                          Map<AclBinding, Integer> results) {
        return new AclEvent<>(auditedEntities, clusterAuditInfo, results, EventType.DESCRIBE);
    }

    public AclEvent(Set<T> auditedEntities, AuditInfo clusterAuditInfo, Map<R, Integer> operationResults, EventType eventType) {
        this.auditedEntities = Collections.unmodifiableSet(auditedEntities);
        this.clusterAuditInfo = clusterAuditInfo;
        this.eventType = eventType;
        this.operationResults = Collections.unmodifiableMap(operationResults);
    }

    public Set<T> auditedEntities() {
        return auditedEntities;
    }

    public AuditInfo clusterAuditInfo() {
        return clusterAuditInfo;
    }

    public Map<R, Integer> operationResults() {
        return operationResults;
    }

    public EventType eventType() {
        return eventType;
    }
}