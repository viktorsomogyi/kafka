package org.apache.kafka.common.security.audit.types;

import org.apache.kafka.common.security.audit.AuditEventType;

public class NewProducerDetectedEvent implements AuditEventType {

    private String connectionId;

    public NewProducerDetectedEvent(String connectionId) {
        this.connectionId = connectionId;
    }

    public String connectionId() {
        return connectionId;
    }
}
