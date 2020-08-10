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

//import com.github.benmanes.caffeine.cache.Caffeine;
//import com.google.common.cache.CacheLoader;
//import com.google.common.cache.LoadingCache;
//import com.google.common.cache.RemovalListener;
//import org.apache.kafka.common.Endpoint;
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.acl.AclBinding;
//import org.apache.kafka.common.acl.AclBindingFilter;
//import org.apache.kafka.common.protocol.Errors;
//import org.apache.kafka.server.authorizer.AclCreateResult;
//import org.apache.kafka.server.authorizer.AclDeleteResult;
//import org.apache.kafka.server.authorizer.Action;
//import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
//import org.apache.kafka.server.authorizer.AuthorizationResult;
//import org.apache.kafka.server.authorizer.Authorizer;
//import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.CompletionStage;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.TimeUnit;
//
//public class FlowAuditor implements Auditor {
//
//    private static final Logger LOG = LoggerFactory.getLogger(FlowAuditor.class);
//
//    private long producerCacheExpiryMs = 10000;
//    private int producerCacheMaxSize = 100000;
//
//    private CacheLoader<String, Set<TopicPartition>> cacheLoader = new CacheLoader<String, Set<TopicPartition>>() {
//        @Override
//        public Set<TopicPartition> load(String key) {
//            return ConcurrentHashMap.newKeySet();
//        }
//    };
//    private RemovalListener<String, Set<TopicPartition>> removalListener = notification -> {
//        LOG.debug("Removing client {} from {}", notification.getKey(), notification.getCause());
//    };
//
////    private LoadingCache<String, Set<TopicPartition>> producerCache = Caffeine.newBuilder()
////        .expireAfterWrite(producerCacheExpiryMs, TimeUnit.MILLISECONDS)
////        .expireAfterAccess(producerCacheExpiryMs, TimeUnit.MILLISECONDS)
////        .maximumSize(producerCacheMaxSize)
////        .initialCapacity(producerCacheMaxSize/2)
////        .removalListener(removalListener)
////        .build(cacheLoader);
//
//    @Override
//    public void onEvent(AuditEvent event) {
//
//    }
//
//    @Override
//    public void audit(AuthorizableRequestContext requestContext, Action actions, AuthorizationResult authorizationResults, Errors actionResult) {
//
//    }
//
//    @Override
//    public void configure(Map<String, ?> configs) {
//
//    }
//}
