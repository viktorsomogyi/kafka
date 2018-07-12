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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.util.concurrent.ExecutionException;

public class ClusterInfoCommand extends ShellCommand {

    ClusterInfoCommand(AdminClient adminClient) {
        super(adminClient);
    }

    @Override
    void init(Subparser subparser) {

    }

    @Override
    public void execute(Namespace namespace) {
        DescribeClusterResult result = adminClient.describeCluster();
        try {
            String id = result.clusterId().get();
            System.out.println("ClusterID: " + id);
            Node controller = result.controller().get();
            System.out.format("Controller: %s, ", controller.id());
            printNodeData(controller);
            result.nodes().get().forEach(broker -> {
                System.out.format("Broker: %s, ", broker.id());
                printNodeData(broker);
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void printNodeData(Node n) {
        if (n.hasRack()) {
            System.out.format("Address: %s:%s, Rack: %s\n", n.host(), n.port(), n.rack());
        } else {
            System.out.format("Address: %s:%s\n", n.host(), n.port());
        }
    }

    @Override
    public String name() {
        return "cluster";
    }
}
