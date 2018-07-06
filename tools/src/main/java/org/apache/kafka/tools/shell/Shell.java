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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Shell {

    static final String SUBCOMMANDS = "subcommands";
    private static final Logger logger = LoggerFactory.getLogger(Shell.class);
    private Map<String, ShellCommand> subcommands = new HashMap<>();
    private AdminClient adminClient;
    private ArgumentParser parser;

    public static void main(String[] args) {
        Shell shell = new Shell();
        shell.execute(args);
    }

    Shell() {
        Properties properties = getOverrideConfig();
        if (properties == null) {
            logger.debug("No override config found in KAFKA_SHELL_CONFIG");
            properties = getDefaultConfig();
        }
        if (properties == null) {
            logger.debug("No default config found in /etc/kafka-shell.properties");
            properties = getFallbackConfig();
        }
        adminClient = KafkaAdminClient.create(properties);
        parser = argParser();
        createSubcommands();
    }

    void execute(String[] args) {
        try {
            if (args == null || args.length == 0) {
                InteractiveShell interactiveShell = new InteractiveShell(adminClient, null, parser, subcommands);
                interactiveShell.execute((Namespace) null);
            } else {
                Namespace ns = parser.parseArgs(args);
                ShellCommand cmd = subcommands.get(ns.getString(SUBCOMMANDS));
                cmd.execute(ns);
            }
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }
    }

    /**
     * Reads the default config from the specified path. If there is
     * no such file, returns `null`.
     * @return a populated {@link Properties} object if it found the config
     * file, otherwise `null`.
     */
    private Properties getConfig(String pathname) {
        if (pathname == null || pathname.isEmpty()) return null;

        File propsFile = new File(pathname);
        if (!propsFile.exists()) return null;
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(propsFile));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return props;
    }

    private Properties getOverrideConfig() {
        return getConfig(System.getProperty("KAFKA_SHELL_CONFIG"));
    }

    private Properties getDefaultConfig() {
        return getConfig("/etc/kafka-shell.properties");
    }

    private Properties getFallbackConfig() {
        Properties configs = new Properties();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092");
        return configs;
    }

    private ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("kafka")
                .addHelp(true)
                .build();
        return parser;
    }

    private void createSubcommands() {
        Subparsers subparsers = parser.addSubparsers();
        subparsers.dest(SUBCOMMANDS);
        Topics topicsCommand = new Topics(adminClient, subparsers);
        subcommands.put(topicsCommand.name(), topicsCommand);

        Configs configsCommand = new Configs(adminClient, subparsers);
        subcommands.put(configsCommand.name(), configsCommand );

        Logs logsCommand = new Logs(adminClient, subparsers);
        subcommands.put(logsCommand.name(), logsCommand);

        ClusterInfo clusterInfoCommand = new ClusterInfo(adminClient, subparsers);
        subcommands.put(clusterInfoCommand.name(), clusterInfoCommand);

        ConsumerGroups consumerGroups = new ConsumerGroups(adminClient, subparsers);
        subcommands.put(consumerGroups.name(), consumerGroups);

        InteractiveShell interactiveShell = new InteractiveShell(adminClient, subparsers, parser, subcommands);
        subcommands.put(interactiveShell.name(), interactiveShell);
    }
}
