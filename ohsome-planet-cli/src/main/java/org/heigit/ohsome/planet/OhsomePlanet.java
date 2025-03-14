package org.heigit.ohsome.planet;

import org.heigit.ohsome.contributions.Contributions2Parquet;
import org.heigit.ohsome.contributions.FileInfo;
import picocli.CommandLine;

import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;

@Command(name = "ohsome-planet",
        mixinStandardHelpOptions = true,
        version = "ohsome-planet 1.0.0",
        description = "Transform OSM (history) PBF files into GeoParquet. Enrich with OSM changeset metadata and country information.%n",
        subcommands = {
            FileInfo.class,
            Contributions2Parquet.class,
        })
public class OhsomePlanet implements Callable<Integer> {

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    public static void main(String[] args) {
        var main = new OhsomePlanet();
        var exit = new CommandLine(main).execute(args);
        System.exit(exit);
    }
}
