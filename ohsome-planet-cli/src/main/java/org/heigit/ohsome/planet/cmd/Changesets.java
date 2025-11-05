package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.replication.ReplicationManager;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "changesets",
        mixinStandardHelpOptions = true,
        description = "initial database for updates"
)
public class Changesets implements Callable<Integer> {
    @Option(paramLabel = "path_to_changeset.xml", names = {"--changesets"}, description = "initial changeset.osm.bz2 from planet. https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2")
    Path changesetsPath;
    @Option(paramLabel = "conn_url", names = {"--changeset-db"}, description = "full read/write jdbc:url for changeset database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
    String changesetDbUrl;

    @Override
    public Integer call() throws IOException {
        return ReplicationManager.initChangesets(changesetsPath, changesetDbUrl);
    }
}
