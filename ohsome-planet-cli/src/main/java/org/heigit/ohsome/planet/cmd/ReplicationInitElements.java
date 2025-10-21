package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.replication.ReplicationManager;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "elements",
        mixinStandardHelpOptions = true,
        description = "initial database for elements updates")
public class ReplicationInitElements implements Callable<Integer> {

    @Option(paramLabel = "path_to_pbf",names = {"--pbf"}, required = true, description = "initial osm.pbf from planet. https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf")
    private Path pbfPath;

    @Option(paramLabel = "url_for_replication", names = {"--replication_url"}, required = true, description = "url for replication updates")
    private String replicationUrl;

    @Option(paramLabel = "path_to_dir", names = {"--dir"}, required = true, description = "Output directory for key-value latest contribution store")
    private Path directory;
    @Option(paramLabel = "parallel",  names = {"--parallel"},
            required = false,
            description = "number of threads used for initialization.")
    private int parallel = Runtime.getRuntime().availableProcessors() - 1;

    @Override
    public Integer call() throws Exception {
        return ReplicationManager.initElements(pbfPath, directory, replicationUrl, parallel);
    }
}
