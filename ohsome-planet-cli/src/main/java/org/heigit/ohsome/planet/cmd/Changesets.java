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
    static class Output {
        @Option(
                paramLabel = "conn_url",
                names = {"--changeset-db"},
                description = "Full read/write JDBC URL for changeset database, e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD"
        )
        String changesetDbUrl;

        @Option(
                paramLabel = "path",
                names = {"--output"},
                description = "Path to output parquet file (mutually exclusive with --changeset-db)"
        )
        Path output;
    }

    @Option(paramLabel = "path_to_changeset.xml", names = {"--changesets"}, description = "initial changeset.osm.bz2 from planet. https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2")
    Path changesetsPath;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "Output target (choose one)")
    Output output;
    @Option(paramLabel = "overwrite", names = {"--overwrite"}, description = "If set, truncate changeset and changeset_state tables before refilling.")
    boolean overwrite;
    @Option(names = {"-v", "--verbose"}, description = "By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace")
    boolean[] verbosity;


    @Override
    public Integer call() throws IOException, SQLException {
        CliUtils.setVerbosity(verbosity);

        if (output.output != null) {
            return ReplicationManager.initChangesets(changesetsPath, output.output, overwrite);
        }

        return ReplicationManager.initChangesets(changesetsPath, output.changesetDbUrl, overwrite);
    }
}
