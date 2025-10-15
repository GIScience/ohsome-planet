package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.replication.ReplicationManager;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "replication",
        mixinStandardHelpOptions = true,
        description = "",
        subcommands = {
           ReplicationInit.class
        }
)
public class Replication implements Callable<Integer> {
    public enum ReplicationInterval {
        hour, minute, day
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return CommandLine.ExitCode.OK;
    }

    @Command
    public int update(
            @Option(names = {"--country-file"})
            Path countryFilePath,
            @Option(names = {"--changeset-db"}, description = "full jdbc:url to changesetmd database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
            String changesetDbUrl,
            @Option(names = {"--interval"}, description = "Replication file interval. Valid values: ${COMPLETION-CANDIDATES}, default: ${DEFAULT-VALUE}", defaultValue = "minute")
            ReplicationInterval interval,
            @Option(names = {"--parallel"}, description = "number of threads used for processing. Dictates the number of files which will created.")
            int parallel,
            @Option(paramLabel = "path_to_dir", names = {"--directory"}, required = true, description = "Output directory for key-value latest contribution store")
            Path directory,
            @Option(names = {"--output"}, defaultValue = "out", description = "output directory, Default: ${DEFAULT-VALUE}")
            Path out
    ) throws IOException {
        return ReplicationManager.update(interval.toString(), directory, changesetDbUrl);
    }
}
