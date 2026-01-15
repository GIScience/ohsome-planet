package org.heigit.ohsome.planet;

import org.heigit.ohsome.planet.cmd.Changesets;
import org.heigit.ohsome.planet.cmd.Contributions;
import org.heigit.ohsome.planet.cmd.Replication;
import org.heigit.ohsome.planet.cmd.ReplicationStore;
import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static org.heigit.ohsome.contributions.FileInfo.printInfo;
import static picocli.CommandLine.Command;

@Command(name = "ohsome-planet",
        mixinStandardHelpOptions = true,
        versionProvider = ManifestVersionProvider.class,
        description = "Transform OSM (history) PBF files into GeoParquet. Enrich with OSM changeset metadata and country information.%n",
        subcommands = {
                Contributions.class,
                Changesets.class,
                Replication.class,
                ReplicationStore.class
        })
public class OhsomePlanet implements Callable<Integer> {

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    @Command(name = "fileinfo",
            description = "print header for osm pbf file")
    public int fileInfo(@Option(names = {"--pbf"}, required = true) Path path) throws IOException {
        printInfo(path);
        return CommandLine.ExitCode.OK;
    }

    public static void main(String[] args) {
        var main = new OhsomePlanet();
        var exit = new CommandLine(main).execute(args);
        System.exit(exit);
    }
}
