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

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;


public class InteractiveShell {

    private static final String COMMAND = "command";
    private ArgumentParser parser;
    private Map<String, ShellCommand> subcommands;

    InteractiveShell(ArgumentParser parser,
                            Map<String, ShellCommand> subcommands) {
        this.parser = parser;
        this.subcommands = subcommands;
    }

    public void run() {
        System.out.println("Type 'q', 'quit' or 'exit' to leave the Kafka shell");
        try {
            shell: while (true) {
                System.out.print("\nkafka> ");
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    String[] args = reader.readLine().split(" ");
                    for (String arg : args) {
                        if ("q".equals(arg) || "quit".equals(arg) || "exit".equals(arg))
                            break shell;
                    }
                    execute(args);
            }
        } catch (IOException e) {
            e.printStackTrace();
            Exit.exit(1);
        }
    }

    private void execute(String[] args) {
        try {
            Namespace ns = parser.parseArgs(args);
            String command = ns.getString(COMMAND);
            if ("q".equals(command) || "quit".equals(command)) {
                Exit.exit(0);
            } else {
                ShellCommand cmd = subcommands.get(command);
                cmd.execute(ns);
            }
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }
    }
}
