package org.apache.kafka.tools.shell;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.AdminClient;

public abstract class ShellCommand {

    AdminClient adminClient;
    Subparsers subparsers;

    public ShellCommand(AdminClient adminClient, Subparsers subparsers) {
        this.adminClient = adminClient;
        this.subparsers = subparsers;
    }

    public abstract void execute(Namespace namespace);

    public abstract String name();
}
