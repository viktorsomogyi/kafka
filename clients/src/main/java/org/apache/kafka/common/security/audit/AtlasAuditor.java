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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.security.audit.types.TopicCreatedEvent;
import org.apache.kafka.common.security.audit.types.TopicDeletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class AtlasAuditor implements Auditor {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuditor.class.getName());

    private static final String ATLAS_ENDPOINT = "atlas.rest.address";

    private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";
    private static final String FORMAT_KAKFA_TOPIC_QUALIFIED_NAME = "%s@%s";
    private static final String KAFKA_METADATA_NAMESPACE = "atlas.metadata.namespace";
    private static final String CLUSTER_NAME_KEY = "atlas.cluster.name";
    private static final String DEFAULT_CLUSTER_NAME = "primary";
    private static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
    private static final String DESCRIPTION_ATTR = "description";
    private static final String PARTITION_COUNT = "partitionCount";
    private static final String REPLICATION_FACTOR = "replicationFactor";
    private static final String NAME = "name";
    private static final String URI = "uri";
    private static final String CLUSTERNAME = "clusterName";
    private static final String TOPIC = "topic";
    private static final String KAFKA_TOPIC = "kafka_topic";
    public static final String AUDITOR_ADMIN_CLIENT_CONFIG_PREFIX = "auditors.admin.";

    private final AtlasClientV2 atlasClientV2;
    private final String metadataNamespace;
    private final Executor asyncTaskExecutor;

    public AtlasAuditor() {
        try {
            Configuration atlasConf = ApplicationProperties.get();
            String[] urls = atlasConf.getStringArray(ATLAS_ENDPOINT);

            if (urls == null || urls.length == 0) {
                urls = new String[]{DEFAULT_ATLAS_URL};
            }
            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                atlasClientV2 = new AtlasClientV2(urls, new String[]{"admin", "admin123"});
            } else {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), urls);
            }
            metadataNamespace = getMetadataNamespace(atlasConf);
            asyncTaskExecutor = Executors.newSingleThreadExecutor();
        } catch (AtlasException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEvent(AuditEvent event) {
        try {
            if (event.auditEventType() instanceof TopicCreatedEvent) {
                onTopicCreated(event.requestContext(), (TopicCreatedEvent) event.auditEventType());
            } else if (event.auditEventType() instanceof TopicDeletedEvent) {
                onTopicDeleted(event.requestContext(), (TopicDeletedEvent) event.auditEventType());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Properties config = parseAdminClientConfigs(configs);
        Admin admin = KafkaAdminClient.create(config);
        asyncTaskExecutor.execute(() -> {
            try {
                Set<String> topics = admin.listTopics().names().get();
                Set<String> qualifiedTopics = topics.stream().map(t -> getTopicQualifiedName(metadataNamespace, t)).collect(Collectors.toSet());
                qualifiedTopics.forEach(topic -> {
                    try {
                        AtlasEntity.AtlasEntitiesWithExtInfo atlasEntitiesWithExtInfo = atlasClientV2.getEntitiesByAttribute(KAFKA_TOPIC, qualifiedTopics
                            .stream()
                            .map(t -> Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, t))
                            .collect(Collectors.toList()));

                        List<AtlasEntity> queriedAtlasEntities = atlasEntitiesWithExtInfo.getEntities();
                        List<AtlasEntity> atlasEntities = queriedAtlasEntities == null ? new ArrayList<>() : queriedAtlasEntities;
                        List<AtlasEntity> entitiesToBeDeleted = atlasEntities
                            .stream()
                            .filter(ae -> !qualifiedTopics.contains(ae.getAttribute(ATTRIBUTE_QUALIFIED_NAME)))
                            .collect(Collectors.toList());
                        List<String> topicsToBeCreated = qualifiedTopics
                            .stream()
                            .filter(t -> atlasEntities
                                .stream()
                                .noneMatch(ae -> ae.getAttribute(ATTRIBUTE_QUALIFIED_NAME).equals(t)))
                            .collect(Collectors.toList());
//                        List<AtlasEntity> topicsToBeUpdated = atlasEntities
//                            .stream()
                        LOG.info("Topic entities to be deleted from Atlas: {}", entitiesToBeDeleted);
                        LOG.info("Topic entities to be created in Atlas: {}", topicsToBeCreated);
                        if (!entitiesToBeDeleted.isEmpty()) {
                            atlasClientV2.deleteEntitiesByGuids(entitiesToBeDeleted
                                .stream()
                                .map(AtlasEntity::getGuid)
                                .collect(Collectors.toList()));
                        }
                        if (!topicsToBeCreated.isEmpty()) {
                            List<String> simpleTopicNames = topicsToBeCreated
                                .stream()
                                .map(AtlasAuditor::getSimpleTopicName)
                                .collect(Collectors.toList());
                            Map<String, KafkaFuture<TopicDescription>> topicDescriptionMap = admin
                                .describeTopics(simpleTopicNames)
                                .values();
                            List<AtlasEntity> atlasEntitiesToBeCreated = topicsToBeCreated
                                .stream()
                                .map(qt -> {
                                    try {
                                        TopicDescription td = topicDescriptionMap.get(getSimpleTopicName(qt)).get();
                                        return atlasTopicEntity(td.name(), td.partitions().size(), td.partitions().get(0).replicas().size()).getEntity();
                                    } catch (InterruptedException | ExecutionException e) {
                                        throw new KafkaException(e);
                                    }
                                })
                                .collect(Collectors.toList());
                            atlasClientV2.createEntities(new AtlasEntity.AtlasEntitiesWithExtInfo(atlasEntitiesToBeCreated));
                        }
                    } catch (AtlasServiceException e) {
                        e.printStackTrace();
                    }
                });
            } catch (InterruptedException | ExecutionException e) {
                throw new KafkaException(e);
            } finally {
                admin.close();
            }
        });
    }

    static Properties parseAdminClientConfigs(Map<String, ?> configMap) {
        Properties props = new Properties();
        for (Map.Entry<String, ?> entry : configMap.entrySet()) {
            if (entry.getKey().startsWith(AUDITOR_ADMIN_CLIENT_CONFIG_PREFIX)) {
                props.put(entry.getKey().replace(AUDITOR_ADMIN_CLIENT_CONFIG_PREFIX, ""), entry.getValue());
            }
        }
        return props;
    }

    private String getMetadataNamespace(Configuration config) {
        return config.getString(KAFKA_METADATA_NAMESPACE, getClusterName(config));
    }

    private String getClusterName(Configuration config) {
        return config.getString(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
    }

    void onTopicCreated(RequestContext context, TopicCreatedEvent event) throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo entity = atlasTopicEntity(event.topicName(), event.partitions(), event.replicationFactor());

        AtlasEntity.AtlasEntityWithExtInfo ret;

        EntityMutationResponse response = atlasClientV2.createEntity(entity);
        List<AtlasEntityHeader> entities = response.getCreatedEntities();

        if (CollectionUtils.isNotEmpty(entities)) {
            AtlasEntity.AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2.getEntityByGuid(entities.get(0).getGuid());

            ret = getByGuidResponse;

            LOG.info("Created {} topic entity: name={}, guid={}", ret.getEntity().getTypeName(), ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
        } else {
            LOG.warn("Couldn't create topic entity {} in Atlas", event.topicName());
        }
    }

    void onTopicDeleted(RequestContext context, TopicDeletedEvent event) throws AtlasServiceException {
        AtlasEntity.AtlasEntityWithExtInfo entity = atlasClientV2.getEntityByAttribute(KAFKA_TOPIC,
            Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, getTopicQualifiedName(metadataNamespace, event.topicName())));
        EntityMutationResponse response = atlasClientV2.deleteEntityByGuid(entity.getEntity().getGuid());
        List<AtlasEntityHeader> entities = response.getDeletedEntities();
        if (CollectionUtils.isNotEmpty(entities)) {
            entities.forEach(e -> LOG.info("Deleted topic entity {} with guid={}", event.topicName(), e.getGuid()));
        } else {
            LOG.warn("Couldn't delete topic {} from Atlas", event.topicName());
        }
    }

    private AtlasEntity.AtlasEntityWithExtInfo atlasTopicEntity(String topicName, int partitions, int replicationFactor) {
        final AtlasEntity ent = new AtlasEntity(KAFKA_TOPIC);

        String qualifiedName = getTopicQualifiedName(metadataNamespace, topicName);

        ent.setAttribute(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
        ent.setAttribute(CLUSTERNAME, metadataNamespace);
        ent.setAttribute(TOPIC, topicName);
        ent.setAttribute(NAME, topicName);
        ent.setAttribute(DESCRIPTION_ATTR, topicName);
        ent.setAttribute(URI, topicName);
        ent.setAttribute(PARTITION_COUNT, partitions);
        ent.setAttribute(REPLICATION_FACTOR, replicationFactor);
        return new AtlasEntity.AtlasEntityWithExtInfo(ent);
    }

    static String getTopicQualifiedName(String metadataNamespace, String topic) {
        return String.format(FORMAT_KAKFA_TOPIC_QUALIFIED_NAME, topic, metadataNamespace);
    }

    static String getSimpleTopicName(String topicName) {
        int clusterNameSeparatorIdx = topicName.indexOf('@');
        if (clusterNameSeparatorIdx > -1) {
            return topicName.substring(0, clusterNameSeparatorIdx);
        } else {
            return topicName;
        }
    }

    public static void main(String[] args) {
        AtlasAuditor auditor = new AtlasAuditor();
        auditor.onEvent(new AuditEvent(null, new TopicDeletedEvent("test1-xxx", Errors.NONE)));
    }
}
