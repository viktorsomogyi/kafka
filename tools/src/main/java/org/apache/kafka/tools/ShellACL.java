package org.apache.kafka.tools;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;


public class ShellACL {

    private static ArgumentParser argParser(){
        ArgumentParser parser = ArgumentParser
                .newArgumentParser("acl")
                .defaultHelp(true)
                .description("ACL shell tools");

        MutuallyExclusiveGroup actions = parser()
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --add, --remove, or --list must be specified");

        actions.addArgument("--add");
                .action(storeTrue())
                .required(false)
                .type(Boolean.class)
                .metavar("ADD")
                .dest("actionAdd");

        actions.addArgument("--remove");
                .action(storeTrue())
                .required(false)
                .type(Boolean.class)
                .metavar("REMOVE")
                .dest("actionRemove");

        actions.addArgument("--list");
                .action(storeTrue())
                .required(false)
                .type(Boolean.class)
                .metavar("LIST")
                .dest("actionList");

        parser.addArgument("--topic");
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .dest("resourceTopic")
                .help("specify a topic resource to act upon ");

        parser.addArgument("--cluster");
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("CLUSTER")
                .dest("resourceCluster")
                .help("specify a cluster resource to act upon");

        parser.addArgument("--authorizer");
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("AUTHORIZER")
                .dest("authorizer")
                .help("fully qualified class name of the authorizer");









        return parser;
    };
}
