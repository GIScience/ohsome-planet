package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.replication.ReplicationManager;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "replication",
        mixinStandardHelpOptions = true,
        description = "")
public class Replication implements Callable<Integer> {
    public enum ReplicationInterval {
        hour, minute, day
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return CommandLine.ExitCode.OK;
    }

    @Command(description = "initial database for updates")
    public int init(
            @Option(paramLabel = "path_to_changeset.xml",names = {"--changesets"}, description = "initial changeset.osm.bz2 from planet. https://planet.openstreetmap.org/planet/")
            Path changesetsPath,
            @Option(paramLabel = "conn_url",names = {"--changeset-db"}, description = "full read/write jdbc:url for changeset database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
            String changesetDbUrl,
            @Option(paramLabel = "path_to_pbf",names = {"--pbf"}, required = true, description = "path to osm/osh pbf file")
            Path pbfPath,
            @Option(paramLabel = "path_to_dir", names = {"--dir"}, required = true, description = "Output directory for key-value latest contribution store")
            Path directory
    ) {
        return new ReplicationManager().init(changesetsPath, changesetDbUrl, pbfPath, directory);
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
    ) {
        return new ReplicationManager().update(interval.toString());
    }
}
