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

package org.apache.kafka.common.security.audit;

import org.apache.kafka.common.requests.RequestContext;

import java.util.UUID;

public class AuditEvent {

    private final RequestContext requestContext;
    private final AuditEventType auditEventType;
    private final UUID uuid;

    public AuditEvent(RequestContext requestContext, AuditEventType auditEventType) {
        this.requestContext = requestContext;
        this.auditEventType = auditEventType;
        this.uuid = UUID.randomUUID();
    }

    public UUID uuid() {
        return uuid;
    }

    public RequestContext requestContext() {
        return requestContext;
    }

    public AuditEventType auditEventType() {
        return auditEventType;
    }

    @Override
    public String toString() {
        return "AuditEvent{" +
            "kafkaPrincipal=" + requestContext.principal +
            ", clientId=" + requestContext.header.clientId() +
            ", auditEventType=" + auditEventType +
            '}';
    }
}
