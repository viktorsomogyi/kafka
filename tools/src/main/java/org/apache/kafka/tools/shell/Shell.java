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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Shell {

    private static final String COMMAND = "command";
    private static final Logger logger = LoggerFactory.getLogger(Shell.class);
    private Map<String, ShellCommand> commands = new HashMap<>();
    private AdminClient adminClient;
    private ArgumentParser parser;

    public static void main(String[] args) {
        Shell shell = new Shell();
        shell.execute(args);
    }

    private Shell() {
        Properties properties = getOverrideProperties();
        if (properties == null) {
            logger.debug("No override properties found in KAFKA_SHELL_PROPERTIES");
            properties = getOverrideConfig();
        }
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
        createCommands();
    }

    private void execute(String[] args) {
        try {
            if (args == null || args.length == 0) {
                InteractiveShell interactiveShell = new InteractiveShell(parser, commands);
                interactiveShell.run();
            } else {
                Namespace ns = parser.parseArgs(args);
                ShellCommand cmd = commands.get(ns.getString(COMMAND));
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

    private Properties getOverrideProperties() {
        String properties = System.getProperty("KAFKA_SHELL_PROPERTIES");
        if (properties == null) return null;
        Properties props = new Properties();
        for (String prop : properties.split(",")) {
            String[] propKeyValue = prop.split("=");
            props.put(propKeyValue[0], propKeyValue[1]);
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
        return ArgumentParsers
                .newFor("kafka")
                .addHelp(true)
                .build();
    }

    private void createCommands() {
        Subparsers subparsers = parser.addSubparsers();
        subparsers.dest(COMMAND);

        TopicsCommand topicsCommand = new TopicsCommand(adminClient);
        topicsCommand.register(commands, subparsers);

        ConfigsCommand configsCommand = new ConfigsCommand(adminClient);
        configsCommand.register(commands, subparsers);

        LogsCommand logsCommand = new LogsCommand(adminClient);
        logsCommand.register(commands, subparsers);

        ClusterInfoCommand clusterInfoCommand = new ClusterInfoCommand(adminClient);
        clusterInfoCommand.register(commands, subparsers);

        ConsumerGroupsCommand consumerGroups = new ConsumerGroupsCommand(adminClient);
        consumerGroups.register(commands, subparsers);
    }
}
