package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import org.heigit.ohsome.replication.rocksdb.UpdateStoreRocksDb;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "replication-store",
        mixinStandardHelpOptions = true,
        versionProvider = ManifestVersionProvider.class,
        description = ""
)
public class ReplicationStore implements Callable<Integer> {

    @Option(names = {"--data"}, required = true)
    private Path data;
    @CommandLine.Parameters
    private List<String> params;


    public Integer call() throws Exception {
        UpdateStoreRocksDb.query(data.resolve("replication"), params);
        return 0;
    }
}
