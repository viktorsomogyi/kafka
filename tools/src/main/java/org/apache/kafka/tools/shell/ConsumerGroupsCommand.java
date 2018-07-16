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

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class ConsumerGroupsCommand extends ShellCommand {

    private static final String CONSUMER_GROUP_OPTIONS = "groupsOptions";
    private static final String DELETE = "delete";
    private static final String LIST = "list";
    private static final String DESCRIBE = "describe";

    ConsumerGroupsCommand(AdminClient adminClient) {
        super(adminClient);
    }

    @Override
    void init(Subparser subparser) {
        subparser.description("Provides consumer groups administration commands");

        Subparsers groupOptions = subparser.addSubparsers();
        groupOptions.dest(CONSUMER_GROUP_OPTIONS);

        Subparser describe = groupOptions.addParser(DESCRIBE);
        describe
                .addArgument("-g", "--group")
                .type(String.class)
                .required(true)
                .action(store())
                .help("This is the name of the group to describe");

        Subparser delete = groupOptions.addParser(DELETE);
        delete
                .addArgument("-g", "--group")
                .type(String.class)
                .required(true)
                .action(store())
                .help("This is the name of the group to delete");

        groupOptions.addParser(LIST);
    }

    @Override
    public void execute(Namespace namespace) {
        try {
            switch (namespace.getString(CONSUMER_GROUP_OPTIONS)) {
                case DELETE:
                    delete(namespace);
                    break;
                case DESCRIBE:
                    describe(namespace);
                    break;
                case LIST:
                    list();
                    break;
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void list() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult result = adminClient.listConsumerGroups();
        result.all().get().forEach(cgl -> System.out.println(cgl.groupId()));
    }

    private void delete(Namespace ns) {
        String group = ns.getString("group");
        DeleteConsumerGroupsResult result = adminClient.deleteConsumerGroups(Collections.singleton(group));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void describe(Namespace ns) throws ExecutionException, InterruptedException {
        String group = ns.getString("group");

        DescribeConsumerGroupsResult groupResult;

        ListConsumerGroupOffsetsResult offsetsResult;
        offsetsResult = adminClient.listConsumerGroupOffsets(group);
        groupResult = adminClient.describeConsumerGroups(Collections.singleton(group));

        groupResult.all().get().forEach((groupName, consumerGroupDescription) -> {
            String consumerType;
            if (!consumerGroupDescription.isSimpleConsumerGroup()) {
                consumerType = "NEW";
            } else {
                consumerType = "SIMPLE";
            }
            System.out.format("\tGROUP: %s \n", group);
            System.out.format("\tCONSUMER-TYPE: %s \n", consumerType);
            System.out.format("\tCOORDINATOR-HOST: %s \n \n ", consumerGroupDescription.coordinator());
        });
        try {
            Map<TopicPartition, OffsetAndMetadata> offset = offsetsResult.partitionsToOffsetAndMetadata().get();
            offset.forEach((topicPartition, offsetAndMetadata) -> {
                System.out.format("\tTOPIC-PARTITION: %s-%s \t CURRENT-OFFSET: %s \n", topicPartition.topic(), topicPartition.partition(), offsetAndMetadata.offset());
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    @Override
    public String name() {
        return "groups";
    }
};