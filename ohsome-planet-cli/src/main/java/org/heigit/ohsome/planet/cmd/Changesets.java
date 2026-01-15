package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.changesets.Changesets2DB;
import org.heigit.ohsome.planet.utils.CliUtils;
import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "changesets",
        mixinStandardHelpOptions = true,
        versionProvider = ManifestVersionProvider.class,
        sortSynopsis = false,
        requiredOptionMarker = '*',
        usageHelpWidth = 120,
        description = """
            Import OSM changesets .bz2 file to PostgreSQL.
            """
)
public class Changesets implements Callable<Integer> {
    @Option(paramLabel = "path/to/changeset.osm.bz2",
            names = {"--bz2"}, required = true,
            description = "Path to osm changeset bz2 file. You can e.g. download it from from https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2 .")
    private Path changesetsPath;

    @CommandLine.Option(names = {"--changeset-db"},
            required = true,
            paramLabel = "jdbc:postgresql://HOST[:PORT]/DATABASE",
            description = """
                    Connection url to ohsome-planet changeset database.
                    Set connections parameters including credentials via jdbc:url "jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD"
                    or via environment variables (OHSOME_PLANET_DB_USER, OHSOME_PLANET_DB_PASSWORD, OHSOME_PLANET_DB_SCHEMA, OHSOME_PLANET_DB_POOLSIZE (default 10)).
                    """)
    private String changesetDbUrl;

    @Option(paramLabel = "overwrite", names = {"--overwrite"}, description = "If set, truncate changeset and changeset_state tables before refilling.")
    private boolean overwrite;

    @Option(names = {"-v"}, description = "By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace")
    private boolean[] verbosity;

    @Option(names = {"--create-tables", "-c"}, description = "Set this flag if you do not have created the changeset table schema yet.")
    private boolean createSchema;

    @Override
    public Integer call() throws IOException, SQLException {
        CliUtils.setVerbosity(verbosity);
        return Changesets2DB.initChangesets(changesetsPath, changesetDbUrl, overwrite, createSchema);
    }
}
