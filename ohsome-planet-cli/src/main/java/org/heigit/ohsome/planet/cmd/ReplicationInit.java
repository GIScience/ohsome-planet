package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.replication.ReplicationManager;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "init",
        mixinStandardHelpOptions = true,
        description = "",
        subcommands = {
                ReplicationInitElements.class
        }
)
public class ReplicationInit implements Callable<Integer> {

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return CommandLine.ExitCode.OK;
    }

    @Command(description = "initial database for updates")
    public int changesets(
            @Option(paramLabel = "path_to_changeset.xml", names = {"--changesets"}, description = "initial changeset.osm.bz2 from planet. https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2")
            Path changesetsPath,
            @Option(paramLabel = "conn_url", names = {"--changeset-db"}, description = "full read/write jdbc:url for changeset database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
            String changesetDbUrl
    ) throws IOException {
        return ReplicationManager.init(changesetsPath, changesetDbUrl);
    }
}
