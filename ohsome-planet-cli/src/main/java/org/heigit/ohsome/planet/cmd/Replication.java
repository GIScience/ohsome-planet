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
    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return CommandLine.ExitCode.OK;
    }

    static class ContributionParameters {
        @Option(names = {"--country-file"})
        Path countryFilePath;
        @Option(names = {"--output"}, defaultValue = "out", description = "output directory for parquet files, Default: ${DEFAULT-VALUE}", required = true)
        Path out;
        @Option(paramLabel = "path_to_dir", names = {"--directory"}, description = "Output directory for key-value latest contribution store", required = true)
        Path directory;

        @Option(names = {"--size"}, description = "Maximum size of change to apply at once. Default: unlimited")
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

    @Command
    public int init(
            @CommandLine.ArgGroup(multiplicity = "1")
            ContributionParameters contributionParameters,

            @CommandLine.Option(names = {"--pbf"}, required = true)
            Path pbfPath) {

        return 0;

    }

    @Command(mixinStandardHelpOptions = true)
    int store(
        @Option(names = {"--directory"}, required = true)
        Path directory,
        @CommandLine.Parameters
        List<String> params) throws Exception {
        UpdateStoreRocksDb.query(directory, params);
        return 0;
    }

    @Command(mixinStandardHelpOptions = true)
    public int update(
            @Option(names = "--continuous", defaultValue = "false", description = "continuous updates")
            boolean continuous,

            @Option(names = {"--parallel"}, description = "number of threads used for processing. Dictates the number of files which will created.")
            int parallel,

            @Option(names = {"-v", "--verbose"}, description = "By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace")
            boolean[] verbosity,

            @CommandLine.ArgGroup(multiplicity = "1")
            OptionalContributions optionalContributions,

            @CommandLine.ArgGroup(multiplicity = "1")
            OptionalChangesets optionalChangesets
    ) throws Exception {
        CliUtils.setVerbosity(verbosity);

        if (optionalChangesets.justContributions && optionalContributions.justChangesets) {
            throw new InvalidParameterException("Either just-contributions or just-changesets can be specified");
        }

        if (optionalChangesets.justContributions) {
            return ReplicationManager.updateContributions(
                    optionalContributions.contributionParameters.countryFilePath,
                    optionalContributions.contributionParameters.directory,
                    optionalContributions.contributionParameters.out,
                    optionalContributions.contributionParameters.size,
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

        return ReplicationManager.update(
                optionalContributions.contributionParameters.countryFilePath,
                optionalContributions.contributionParameters.directory,
                optionalContributions.contributionParameters.out,
                optionalChangesets.changesetParameters.changesetDbUrl,
                optionalChangesets.changesetParameters.replicationChangesetsUrl,
                continuous
        );
    }
}
