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

import java.util.Map;

abstract class ShellCommand {

    AdminClient adminClient;

    ShellCommand(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    final void register(Map<String, ShellCommand> subcommands, Subparsers subparsers) {
        Subparser subparser = subparsers.addParser(name());
        subcommands.put(name(), this);
        init(subparser);
    }

    abstract void init(Subparser subparser);

    abstract void execute(Namespace namespace);

    abstract String name();
}
