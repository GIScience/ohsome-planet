package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.planet.utils.CliUtils;
import org.heigit.ohsome.replication.ReplicationManager;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
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
    @Option(paramLabel = "override", names = {"--override"}, description = "If set, truncate changeset and changeset_state tables before refilling.")
    boolean override;
    @Option(names = {"-v", "--verbose"}, description = "By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace")
    boolean[] verbosity;


    @Override
    public Integer call() throws IOException, SQLException {
        CliUtils.setVerbosity(verbosity);

        return ReplicationManager.initChangesets(changesetsPath, changesetDbUrl, override);
    }
}
