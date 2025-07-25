package org.heigit.ohsome.planet.cmd;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "replication",
        mixinStandardHelpOptions = true,
        description = "")
public class Replication implements Callable<Integer> {

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return CommandLine.ExitCode.OK;
    }

    @Command
    public int init(
            @Option(names = {"--pbf"}, required = true, description = "path to osm/osh pbf file")
            Path pbf,
            @Option(names = {"-d"}, required = true, description = "directory")
            Path database) {

        return CommandLine.ExitCode.OK;
    }

    @Command
    public int update(
            @Option(names = {"--country-file"})
            Path countryFilePath,
            @Option(names = {"--changeset-db"}, description = "full jdbc:url to changesetmd database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
            String changesetDbUrl,
            @Option(names = {"--interval"}, description = "")
            String interval,
            @Option(names = {"--parallel"}, description = "number of threads used for processing. Dictates the number of files which will created.")
            int parallel,
            @Option(names = {"-d"}, required = true, description = "directory")
            Path database,
            @Option(names = {"--output"}, defaultValue = "out", description = "output directory, Default: ${DEFAULT-VALUE}")
            Path out) {

        System.out.println("out = " + out);
        return CommandLine.ExitCode.OK;
    }
}
