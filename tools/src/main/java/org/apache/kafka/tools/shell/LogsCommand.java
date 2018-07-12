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
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class LogsCommand extends ShellCommand {

    private static final String LOGS_OPTIONS = "logs";
    private static final String DESCRIBE = "describe";

    LogsCommand(AdminClient adminClient) {
        super(adminClient);

    }

    @Override
    void init(Subparser subparser) {
        subparser.description("Provides topic administration commands");

        Subparsers logsOptions = subparser.addSubparsers();
        logsOptions.dest(LOGS_OPTIONS);

        Subparser describe = logsOptions.addParser(DESCRIBE);
        describe
                .addArgument("-b", "--broker")
                .type(Integer.class)
                .required(true)
                .action(store());
        describe
                .addArgument("-t", "--topic")
                .type(String.class)
                .action(store());
        describe
                .addArgument("-p", "--partition")
                .type(Integer.class)
                .action(store());
    }

    @Override
    public void execute(Namespace namespace) {
        switch (namespace.getString(LOGS_OPTIONS)) {
            case DESCRIBE:
                describe(namespace);
                break;
        }
    }

    @Override
    public String name() {
        return "logs";
    }

    private void describe(Namespace ns) {
        Integer broker = ns.getInt("broker");
        String topic = ns.getString("topic");
        Integer partition = ns.getInt("partition");
        if (topic != null) {
            TopicPartitionReplica tpr = new TopicPartitionReplica(topic, partition, broker);
            DescribeReplicaLogDirsResult result = adminClient.describeReplicaLogDirs(Collections.singleton(tpr));
            result.values().forEach((tp, ld) -> {
                try {
                    DescribeReplicaLogDirsResult.ReplicaLogDirInfo logDirInfo = ld.get();
                    System.out.format("Broker:%s", tp.brokerId());
                    System.out.format("\tTopic:%s", tp.topic());
                    System.out.format("\tPartition:%s", tp.partition());
                    System.out.format("\tDir:%s", logDirInfo.getCurrentReplicaLogDir());
                    System.out.format("\tLag:%s\n", logDirInfo.getCurrentReplicaOffsetLag());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        } else {
            DescribeLogDirsResult result = adminClient.describeLogDirs(Collections.singleton(broker));
            result.values().forEach((b, info) -> {
                try {
                    Map<String, DescribeLogDirsResponse.LogDirInfo> ldInfo = info.get();
                    ldInfo.forEach((path, logdir) -> {
                        System.out.format("Log dir: %s\n", path);
                        logdir.replicaInfos.forEach((tp, ri) ->
                                System.out.format("\tTopic: %-30s\tPartition: %-6s\tSize: %-20s\tLag: %s\n",
                                        tp.topic(), tp.partition(), ri.size, ri.offsetLag));
                    });
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
