package org.heigit.ohsome.planet;

import org.heigit.ohsome.planet.cmd.Changesets;
import org.heigit.ohsome.planet.cmd.Contributions;
import org.heigit.ohsome.planet.cmd.Debug;
import org.heigit.ohsome.planet.cmd.Replication;
import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import picocli.CommandLine;

import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;

@Command(name = "ohsome-planet",
        mixinStandardHelpOptions = true,
        versionProvider = ManifestVersionProvider.class,
        usageHelpWidth = 120,
        description = """
               
               The ohsome-planet tool can be used to transforms OSM (history) PBF files and OSM replication OSC files into Parquet format with native GEO support.
               Second, you can use it to turn an OSM changeset file (osm.bz2) into a PostgreSQL database table and keep it up-to-date with the OSM planet replication changeset files.
               """,
        subcommands = {
                Contributions.class,
                Changesets.class,
                Replication.class,
                Debug.class
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
