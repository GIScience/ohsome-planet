package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.planet.utils.CliUtils;
import org.heigit.ohsome.replication.ReplicationManager;
import org.heigit.ohsome.replication.rocksdb.UpdateStoreRocksDb;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "replication",
        mixinStandardHelpOptions = true,
        description = ""
)
public class Replication implements Callable<Integer> {

    public static class ContributionParameters {
        @Option(names = {"--country-file"})
        Path countryFilePath;
        @Option(names = {"--parquet-data"}, description = "output directory for parquet files, Default: ${DEFAULT-VALUE}")
        String parquetData;
        @Option(paramLabel = "path_to_dir", names = {"--data"}, description = "Output directory for key-value latest contribution store", required = true)
        Path data = Path.of("ohsome-planet");

        @Option(names = {"--size"}, description = "Maximum name of osc files to apply at once. Default: unlimited")
        int size = 0;
    }

    public static class OptionalContributions {
        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
        ContributionParameters contributionParameters;

        @Option(names = {"--jcs", "--just-changesets"}, description = "Do not process contributions, just changesets")
        boolean justChangesets;
    }


    private static class ChangesetParameters {
        @Option(names = {"--changeset-db"}, description = "full jdbc:url to changeset database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD", required = true)
        String changesetDbUrl;

        @Option(names = {"--replication-changesets"}, defaultValue = "https://planet.openstreetmap.org/replication/changesets/", description = "Replication endpoint for changesets, Default: ${DEFAULT-VALUE}")
        String replicationChangesetsUrl;

    }

    public static class OptionalChangesets {
        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
        ChangesetParameters changesetParameters;

        @Option(names = {"--jcb", "--just-contributions"}, description = "Do not process contributions, just changesets")
        boolean justContributions;
    }

    @Command(mixinStandardHelpOptions = true)
    int store(
        @Option(names = {"--data"}, required = true)
        Path directory,
        @CommandLine.Parameters
        List<String> params) throws Exception {
        UpdateStoreRocksDb.query(directory.resolve("replication"), params);
        return 0;
    }


    @Option(names = "--continue", defaultValue = "false", description = "continue updates")
    private boolean continuous;

    @Option(names = {"--parallel"}, description = "number of threads used for processing. Dictates the number of files which will created.")
    private int parallel;

    @Option(names = {"-v", "--verbose"}, description = "By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace")
    private boolean[] verbosity;

    @CommandLine.ArgGroup(multiplicity = "1")
    private OptionalContributions optionalContributions;

    @CommandLine.ArgGroup(multiplicity = "1")
    private OptionalChangesets optionalChangesets;

    @Override
    public Integer call() throws Exception {
        CliUtils.setVerbosity(verbosity);

        parallel = parallel == 0 ? Math.max(1, Runtime.getRuntime().availableProcessors()) -1 : parallel;

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
