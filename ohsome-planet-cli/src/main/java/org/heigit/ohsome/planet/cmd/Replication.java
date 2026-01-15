package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.planet.utils.CliUtils;
import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import org.heigit.ohsome.replication.ReplicationManager;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.concurrent.Callable;

@Command(name = "replications",
        versionProvider = ManifestVersionProvider.class,
        mixinStandardHelpOptions = true,
        sortSynopsis = false,
        requiredOptionMarker = '*',
        usageHelpWidth = 120,
        description = """
                Transform OSM replication .osc files into parquet format.
                Keep changeset PostgreSQL database up-to-date.
                """
)
public class Replication implements Callable<Integer> {

    public static class ContributionParameters {
        @CommandLine.Option(names = {"--country-file"},
                paramLabel = "path/to/countries.csv",
                description = """
                        Enrich osm-contributions with country-codes.
                        csv format (id;wkt):
                         id = country-code
                         wkt = geometry in wkt format
                        """)
        Path countryFilePath;

        @CommandLine.Option(names = {"--parquet-data"},
                paramLabel = "s3://BUCKET/PATH",
                description = """
                        Write parquet files into separate output directory in your filesystem or S3 cloud storage.
                        For S3 cloud storage set environment variables (S3_ENDPOINT, S3_KEY_ID, S3_SECRET, S3_REGION).
                        """)
        private String parquetData;

        @CommandLine.Option(names = {"--data"}, required = true,
                paramLabel = "path/to/ohsome-planet-data",
                description = """
                        ohsome-planet working directory for parquet-data (updates) and replication-store (replication).
                        """)
        private Path data;

        @Option(names = {"--size"},
                paramLabel = "60",
                description = """
                        Maximum name of osc files to apply at once. Default: unlimited.
                        This can be useful in the non-continue mode.
                        """)
        private int size = 0;
    }

    public static class OptionalContributions {
        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
        ContributionParameters contributionParameters;

        @Option(names = {"--jcs", "--just-changesets"},
                description = """
                        Do not process contributions, just changesets.
                        """)
        boolean justChangesets;
    }


    private static class ChangesetParameters {
        @CommandLine.Option(names = {"--changeset-db"},
                paramLabel = "jdbc:postgresql://HOST[:PORT]/DATABASE",
                required = true,
                description = """
                        Enrich osm-contributions with osm-changeset information.
                        Connection url to ohsome-planet changeset database.
                        Set connections parameters including credentials via jdbc:url "jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD"
                        or via environment variables (OHSOME_PLANET_DB_USER, OHSOME_PLANET_DB_PASSWORD, OHSOME_PLANET_DB_SCHEMA, OHSOME_PLANET_DB_POOLSIZE (default 10)).
                        """)
        private String changesetDbUrl;

        @Option(names = {"--replication-changesets"},
                defaultValue = "https://planet.openstreetmap.org/replication/changesets/",
                description = """
                        Replication endpoint for changesets, Default: ${DEFAULT-VALUE}.
                        """)
        private String replicationChangesetsUrl;

    }

    public static class OptionalChangesets {
        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
        ChangesetParameters changesetParameters;

        @Option(names = {"--jcb", "--just-contributions"},
                description = """
                        Do not process contributions, just changesets.
                        """)
        boolean justContributions;
    }

    @Option(names = "--continue", defaultValue = "false",
            description = """
                    Run continuous updates. If not set, will update to the latest available state on replication server as of processing start time.
                    """)
    private boolean continuous;

    @Option(names = {"--parallel"},
            description = """
                    Number of threads used for processing.
                    Defaults to all available cpus minus 1.
                    """)
    private int parallel;

    @Option(names = {"-v"}, description = """
            By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace .
            """)
    private boolean[] verbosity;

    @CommandLine.ArgGroup(multiplicity = "1")
    private OptionalContributions optionalContributions;

    @CommandLine.ArgGroup(multiplicity = "1")
    private OptionalChangesets optionalChangesets;

    @Override
    public Integer call() throws Exception {
        CliUtils.setVerbosity(verbosity);

        parallel = parallel == 0 ? Math.max(1, Runtime.getRuntime().availableProcessors()) - 1 : parallel;

        if (optionalChangesets.justContributions && optionalContributions.justChangesets) {
            throw new InvalidParameterException("Either just-contributions or just-changesets can be specified");
        }

        if (optionalChangesets.justContributions) {


            var parquetData = optionalContributions.contributionParameters.parquetData;
            if (parquetData == null) {
                parquetData = optionalContributions.contributionParameters.data.resolve("updates").toString();
            }

            return ReplicationManager.updateContributions(
                    optionalContributions.contributionParameters.countryFilePath,
                    optionalContributions.contributionParameters.data,
                    parquetData,
                    optionalContributions.contributionParameters.size,
                    parallel,
                    continuous
            );
        }

        if (optionalContributions.justChangesets) {
            return ReplicationManager.updateChangesets(
                    optionalChangesets.changesetParameters.changesetDbUrl,
                    optionalChangesets.changesetParameters.replicationChangesetsUrl,
                    continuous
            );
        }

        var parquetData = optionalContributions.contributionParameters.parquetData;
        if (parquetData == null) {
            parquetData = optionalContributions.contributionParameters.data.resolve("updates").toString();
        }

        return ReplicationManager.update(
                optionalContributions.contributionParameters.countryFilePath,
                optionalContributions.contributionParameters.data,
                parquetData,
                optionalContributions.contributionParameters.size,
                optionalChangesets.changesetParameters.changesetDbUrl,
                optionalChangesets.changesetParameters.replicationChangesetsUrl,
                continuous
        );
    }
}
