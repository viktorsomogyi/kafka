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

import static net.sourceforge.argparse4j.impl.Arguments.store;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopicsCommand extends ShellCommand {

    private static final String TOPICS_OPTIONS = "topicsOptions";
    private static final String CREATE = "create";
    private static final String DELETE = "delete";
    private static final String LIST = "list";
    private static final String DESCRIBE = "describe";

    TopicsCommand(AdminClient adminClient) {
        super(adminClient);
    }

    @Override
    void init(Subparser subparser) {
        subparser.description("Provides topic administration commands");

        Subparsers topicsOptions = subparser.addSubparsers();
        topicsOptions.dest(TOPICS_OPTIONS);

        Subparser create = topicsOptions.addParser(CREATE);
        create
                .addArgument("-n", "--name")
                .type(String.class)
                .required(true)
                .action(store())
                .help("This is the name of the topic to create");
        create
                .addArgument("-r", "--replicas")
                .type(Short.class)
                .action(store())
                .required(true)
                .help("This is the replication factor of the partitions of the topic to create");
        create
                .addArgument("-p", "--partitions")
                .type(Integer.class)
                .action(store())
                .required(true)
                .help("This is the number of partitions of the topic to create");

        Subparser delete = topicsOptions.addParser(DELETE);
        delete
                .addArgument("-n", "--name")
                .type(String.class)
                .required(true)
                .action(store());

        Subparser describe = topicsOptions.addParser(DESCRIBE);
        describe
                .addArgument("-n", "--name")
                .type(String.class)
                .action(store());

        topicsOptions.addParser(LIST);
    }

    @Override
    public void execute(Namespace namespace) {
        try {
            switch (namespace.getString(TOPICS_OPTIONS)) {
                case CREATE:
                    create(namespace);
                    break;
                case DELETE:
                    delete(namespace);
                    break;
                case DESCRIBE:
                    describe(namespace);
                    break;
                case LIST:
                    list();
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

    @Override
    public String name() {
        return "topics";
    }

    private void create(Namespace ns) {
        String name = ns.getString("name");
        Integer partitions = ns.getInt("partitions");
        Short replicas = ns.getShort("replicas");

        NewTopic topic = new NewTopic(name, partitions, replicas);

        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(topic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void delete(Namespace ns) {
        String name = ns.getString("name");
        DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(name));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void describe(Namespace ns) throws ExecutionException, InterruptedException {
        String topicName = ns.getString("name");
        DescribeTopicsResult result;
        if (topicName == null) {
            result = adminClient.describeTopics(adminClient.listTopics().names().get());
        } else {
            result = adminClient.describeTopics(Collections.singleton(topicName));
        }
        result.values().forEach((name, future) -> {
            try {
                TopicDescription td = future.get();
                System.out.format("Topic:%s\tPartitionCount:%d%n",    //tReplicationFactor:%dtConfigs:%s%s
                        td.name(), td.partitions().size());
                for (TopicPartitionInfo tp : td.partitions()) {
                    System.out.format("\tTopic: %s", td.name());
                    System.out.format("\tPartition: %s", tp.partition());
                    System.out.format("\tLeader: %s", tp.leader().id());
                    System.out.format("\tReplicas: %s", tp.replicas()
                            .stream()
                            .map(node -> Integer.toString(node.id()))
                            .collect(Collectors.joining(",")));
                    System.out.format("\tIsr: %s%n", tp.isr()
                            .stream()
                            .map(node -> Integer.toString(node.id()))
                            .collect(Collectors.joining(",")));
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private void list() throws ExecutionException, InterruptedException {
        ListTopicsResult result = adminClient.listTopics();
        result.listings().get().forEach(tp -> System.out.println(tp.name()));
    }
}
