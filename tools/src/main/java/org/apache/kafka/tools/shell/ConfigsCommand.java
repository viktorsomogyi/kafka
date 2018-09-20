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

package org.apache.kafka.tools.shell;


import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class ConfigsCommand extends ShellCommand {

    private static final String CONFIGS_OPTIONS = "configsOptions";
    private static final String UPDATE = "update";
    private static final String DELETE = "delete";
    private static final String DESCRIBE = "describe";
    private static final String DELETE_ARG = "DELETE";
    private static final String UPDATE_ARG = "UPDATE";

    ConfigsCommand(AdminClient adminClient) {
        super(adminClient);
    }

    @Override
    void init(Subparser subparser) {
        Subparsers topicsOptions = subparser.addSubparsers();
        topicsOptions.dest(CONFIGS_OPTIONS);

        Subparser describe = topicsOptions.addParser(DESCRIBE);
        addBrokerTopicArgumentGroup(describe);

        Subparser delete = topicsOptions.addParser(DELETE);
        addBrokerTopicArgumentGroup(delete);
        delete.addArgument(DELETE_ARG)
                .action(store())
                .required(true)
                .type(String.class);

        Subparser alter = topicsOptions.addParser(UPDATE);
        addBrokerTopicArgumentGroup(alter);
        alter.addArgument(UPDATE_ARG)
                .action(store())
                .required(true)
                .type(String.class);
    }

    private void addBrokerTopicArgumentGroup(Subparser subparser) {
        MutuallyExclusiveGroup brokerTopicGroup = subparser.addMutuallyExclusiveGroup();
        brokerTopicGroup.required(true);
        brokerTopicGroup.addArgument("-b", "--brokers")
                .help("A string separated list of broker IDs, like '123,124,125'. An empty argument" +
                        "is the same as specifying all brokers.")
                .action(store())
                .type(Integer.class);
        brokerTopicGroup.addArgument("-t", "--topics")
                .help("A string separated list of topics, like 'topic1,topic2,topic3'. An empty argument" +
                        "is the same as specifying all topics.")
                .action(store())
                .type(String.class);
    }

    @Override
    public void execute(Namespace namespace) {
        try {
            switch (namespace.getString(CONFIGS_OPTIONS)) {
                case UPDATE:
                    update(namespace);
                    break;
                case DELETE:
                    delete(namespace);
                    break;
                case DESCRIBE:
                    describe(namespace);
                    break;
                default:
                    // Since the argument parser should have caught the invalid command we don't
                    // need to do anything here
                    break;
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void describe(Namespace namespace) throws ExecutionException, InterruptedException {
        List<ConfigResource> resources = getResourcesFromParameters(namespace);
        DescribeConfigsResult configResults = adminClient.describeConfigs(resources);
        if (configResults != null) {
            configResults.all().get().forEach((cr, c) -> {
                System.out.format("%s %s%n", cr.type(), cr.name());
                c.entries().forEach(configEntry -> System.out.format("\t%s = %s%n", configEntry.name(), configEntry.value()));
            });
        }
    }

    private void delete(Namespace namespace) throws ExecutionException, InterruptedException {
        List<ConfigResource> resources = getResourcesFromParameters(namespace);
        DescribeConfigsResult configResults = adminClient.describeConfigs(resources);
        Set<String> configsToDelete = new HashSet<>(Arrays.asList(namespace.getString(DELETE_ARG).split(",")));

        Map<ConfigResource, Config> resourceMap = new HashMap<>();
        configResults.all().get().forEach((cr, c) -> {
            Set<ConfigEntry> newConfigSet = new HashSet<>(c.entries());
            newConfigSet.removeIf(conf -> configsToDelete.contains(conf.name()));
            Config updatedConfig = new Config(newConfigSet);
            resourceMap.put(cr, updatedConfig);
        });

        AlterConfigsResult result = adminClient.alterConfigs(resourceMap);
        result.all().get();
    }

    private void update(Namespace namespace) throws ExecutionException, InterruptedException {
        List<ConfigResource> resources = getResourcesFromParameters(namespace);
        DescribeConfigsResult configResults = adminClient.describeConfigs(resources);

        Map<String, ConfigEntry> updatedConfigs = new HashMap<>();
        for (String prop : namespace.getString(UPDATE_ARG).split(",")) {
            String[] pieces = prop.split("=");
            if (pieces.length != 2)
                throw new IllegalArgumentException("Invalid property: " + prop);
            updatedConfigs.put(pieces[0], new ConfigEntry(pieces[0], pieces[1]));
        }

        Map<ConfigResource, Config> resourceMap = new HashMap<>();
        configResults.all().get().forEach((cr, c) -> {
            Map<String, ConfigEntry> configs = c.entries()
                    .stream()
                    .filter(conf -> !conf.isDefault())
                    .collect(Collectors.toMap(ConfigEntry::name, entry -> entry));
            configs.putAll(updatedConfigs);
            Config updatedConfig = new Config(configs.values());
            resourceMap.put(cr, updatedConfig);
        });

        AlterConfigsResult result = adminClient.alterConfigs(resourceMap);
        result.all().get();
    }

    private List<ConfigResource> getResourcesFromParameters(Namespace namespace) {
        String brokers = namespace.getString("brokers");
        String topics = namespace.getString("topics");

        ConfigResource.Type resourceType;
        String[] resourceArr;
        if (brokers != null) {
            resourceArr = brokers.split(",");
            resourceType = ConfigResource.Type.BROKER;
        } else if (topics != null) {
            resourceArr = topics.split(",");
            resourceType = ConfigResource.Type.TOPIC;
        } else {
            resourceArr = new String[0];
            resourceType = ConfigResource.Type.UNKNOWN;
        }
        return Arrays.stream(resourceArr)
                .map(id -> new ConfigResource(resourceType, id))
                .collect(Collectors.toList());
    }

    @Override
    public String name() {
        return "configs";
    }
}
