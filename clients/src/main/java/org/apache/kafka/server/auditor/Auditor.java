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

package org.apache.kafka.server.auditor;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.util.List;
import java.util.Map;

/**
 * An auditor class that can be used to hook into the request after its completion and do auditing tasks, such as
 * logging to a special file or sending information about the request to external systems.
 * Threading model:
 * <ul>
 *     <li>The auditor implementation must be thread-safe.</li>
 *     <li>The auditor implementation is expected to be asynchronous with low latency as it is used in performance
 *     sensitive areas, such as in handling produce requests.</li>
 *     <li>Any threads or thread pools used for processing remote operations asynchronously can be started during
 *     start(). These threads must be shutdown during close().</li>
 * </ul>
 */
public interface Auditor extends Configurable, AutoCloseable {

    class AuthorizationInformation {
        private final ResourcePattern resource;
        private final Boolean allowed;
        private final Errors error;

        public static class Builder {
            private ResourcePattern resource;
            private Boolean allowed;
            private Errors error;

            public Builder resource(ResourcePattern resource) {
                this.resource = resource;
                return this;
            }

            public Builder allowed(Boolean allowed) {
                this.allowed = allowed;
                return this;
            }

            public Builder error(Errors error) {
                this.error = error;
                return this;
            }

            public AuthorizationInformation build() {
                return new AuthorizationInformation(resource, allowed, error);
            }
        }

        public AuthorizationInformation(ResourcePattern resource, Boolean allowed, Errors error) {
            this.resource = resource;
            this.allowed = allowed;
            this.error = error;
        }

        public AuthorizationInformation(ResourceType resourceType, String resourceName,
                                        Boolean allowed, Errors error) {
            this.resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
            this.allowed = allowed;
            this.error = error;
        }

        public AuthorizationInformation(ResourceType resourceType, String resourceName, Boolean allowed) {
            this.resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
            this.allowed = allowed;
            this.error = Errors.NONE;
        }

        public ResourcePattern resource() {
            return resource;
        }

        public Boolean allowed() {
            return allowed;
        }

        public Errors error() {
            return error;
        }
    }

    /**
     * Any threads or thread pools can be started in this method. These resources must be closed in the {@link #close()}
     * method.
     */
    void start();

    /**
     * Called on request completion before returning the response to the client. It allows auditing multiple resources
     * in the request, such as multiple topics being created.
     * @param request is the request that has been issued by the client.
     * @param requestContext contains metadata to the request.
     * @param authorizationInformation is the operation, resource and the outcome of the authorization for any resource
     *                                in the request together.
     */
    void audit(AbstractRequest request,
               AuthorizableRequestContext requestContext,
               Map<AclOperation, List<AuthorizationInformation>> authorizationInformation);
}
