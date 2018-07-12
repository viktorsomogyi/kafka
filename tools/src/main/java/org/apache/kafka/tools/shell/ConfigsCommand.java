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

import net.sourceforge.argparse4j.inf.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;


import java.util.*;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ConfigsCommand extends ShellCommand {

    private static final String TOPICS_OPTIONS = "topicsOptions";

    public ConfigsCommand(AdminClient adminClient) {
        super(adminClient);
    }

    @Override
    void init(Subparser subparser) {
        subparser.addArgument("-a", "--add")
                .action(store())
                .required(false)
                .type(String.class);

        subparser.addArgument("--delete")
                .action(store())
                .required(false)
                .type(String.class);

        subparser.addArgument("--describe")
                .action(storeTrue())
                .required(false);

        subparser.addArgument("--broker")
                .action(store())
                .required(false)
                .type(Integer.class);

        subparser.addArgument("--topic")
                .action(store())
                .required(false)
                .type(String.class);
    }

    @Override
    public void execute(Namespace namespace) {
        String add = namespace.getString("add");
        String delete = namespace.getString("delete");
        Boolean describe = namespace.getBoolean("describe");
        Integer broker = namespace.getInt("broker");
        String topic = namespace.getString("topic");


        if (add != null) {
            DescribeConfigsResult configresults = null;

            if (broker != null) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, broker.toString());
                configresults = adminClient.describeConfigs(Collections.singleton(resource));
            } else if (topic != null) {
                ConfigResource topicresource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                configresults = adminClient.describeConfigs(Collections.singleton(topicresource));
            }
            List<ConfigEntry> configlist = new ArrayList<>();
            try {
                configresults.all().get().forEach((cr, c) -> configlist.addAll(c.entries()));
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            for (String prop : add.split(",")) {
                String[] pieces = prop.split("=");
                if (pieces.length != 2)
                    throw new IllegalArgumentException("Invalid property: " + prop);
                configlist.add(new ConfigEntry(pieces[0], pieces[1]));
            }
            if (broker != null) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, broker.toString());
                Map<ConfigResource, Config> name = new HashMap<>();
                name.put(resource, new Config(configlist));
                AlterConfigsResult result = adminClient.alterConfigs(name);
                try {
                    result.all().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            if (topic != null) {

                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                Map<ConfigResource, Config> name = new HashMap<>();
                name.put(resource, new Config(configlist));
                AlterConfigsResult result = adminClient.alterConfigs(name);
                try {
                    result.all().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        } else if (describe) {
            DescribeConfigsResult configresults = null;
            if (broker != null) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, broker.toString());
                configresults = adminClient.describeConfigs(Collections.singleton(resource));
            } else if (topic != null) {
                ConfigResource topicresource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                configresults = adminClient.describeConfigs(Collections.singleton(topicresource));
            }
            if (configresults != null) {
                try {
                    configresults.all().get().forEach((cr, c) -> {
                        System.out.format("%s %s\n", cr.type(), cr.name());
                        c.entries().forEach(configEntry -> System.out.format("\t%s = %s\n", configEntry.name(), configEntry.value()));
                    });

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        } else if (delete != null) {
            DescribeConfigsResult configresults = null;

            if (broker != null) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, broker.toString());
                configresults = adminClient.describeConfigs(Collections.singleton(resource));
            } else if (topic != null) {
                ConfigResource topicresource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                configresults = adminClient.describeConfigs(Collections.singleton(topicresource));
            }
            try {
                if (configresults != null) {
                    List<ConfigEntry> configlist = new ArrayList();
                    Set<String> dellist = new HashSet<>(Arrays.asList(delete.split(",")));
                    configresults.all().get().forEach((cr, c) -> {

                        c.entries().forEach(configEntry -> {
                            if (!dellist.contains(configEntry.name())) {
                                configlist.add(configEntry);
                            }
                        });
                    });

                    if (broker != null) {
                        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, broker.toString());
                        Map<ConfigResource, Config> name = new HashMap<>();
                        name.put(resource, new Config(configlist));
                        AlterConfigsResult result = adminClient.alterConfigs(name);
                        result.all().get();
                    }
                    if (topic != null) {
                        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                        Map<ConfigResource, Config> name = new HashMap<>();
                        name.put(resource, new Config(configlist));
                        AlterConfigsResult result = adminClient.alterConfigs(name);
                        result.all().get();
                    }
                }


            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }


        }
    }

    @Override
    public String name() {
        return "configs";
    }
}
