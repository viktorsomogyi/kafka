package org.apache.kafka.tools.shell;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;

import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;

import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;

import java.util.Collections;
import java.util.concurrent.ExecutionException;


public class ConsumerGroups extends ShellCommand{

    private static final String CONSUMER_GROUP_OPTIONS = "groupsOptions";
    private static final String DELETE = "delete";
    private static final String LIST = "list";
    private static final String DESCRIBE = "describe";

    ConsumerGroups(AdminClient adminClient, Subparsers subparsers) {
        super(adminClient, subparsers);
        Subparser groupOptions = subparsers.addParser(name());
        groupOptions.description("Provides consumer groups administration commands");

        Subparsers groupOptions = groupOptions.addSubparsers();
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
                .help("This is the name of the group to delete");;

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
        //needs to unpack result
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
        DescribeConsumerGroupsResult result;
        if (group == null) {
            adminClient.listConsumerGroups();
            result = adminClient.describeConsumerGroups(); //missing access
        } else {
            result = adminClient.describeConsumerGroups(Collections.singleton(group));
        }

        //need to unpack results

    }


};