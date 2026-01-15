package org.heigit.ohsome.planet.cmd;


import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import org.heigit.ohsome.replication.rocksdb.UpdateStoreRocksDb;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import static org.heigit.ohsome.contributions.FileInfo.printInfo;

@CommandLine.Command(name = "debug",
        versionProvider = ManifestVersionProvider.class,
        mixinStandardHelpOptions = true,
        description = """
                Utility commands for debugging purposes.
                """)
public class Debug implements Callable<Integer> {

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    @CommandLine.Command(name = "fileinfo",
            versionProvider = ManifestVersionProvider.class,
            mixinStandardHelpOptions = true,
            sortSynopsis = false,
            requiredOptionMarker = '*',
            usageHelpWidth = 120,
            description = """
                Print header for osm pbf file.
                """)
    public int fileInfo(@CommandLine.Option(names = {"--pbf"}, required = true) Path path) throws IOException {
        printInfo(path);
        return CommandLine.ExitCode.OK;
    }

    @CommandLine.Command(name = "replication-store",
            versionProvider = ManifestVersionProvider.class,
            mixinStandardHelpOptions = true,
            sortSynopsis = false,
            requiredOptionMarker = '*',
            usageHelpWidth = 120,
            description = """
                    Peek into replication store and show osm element information.
                    """)
    public Integer call(
            @CommandLine.Option(names = {"--data"}, required = true)Path data,
            @CommandLine.Parameters(description = "specify osm element type/id. e.g. n/1234 w/34565 r/4567") List<String> params
    ) throws Exception {
        UpdateStoreRocksDb.query(data.resolve("replication"), params);
        return 0;
    }

}
