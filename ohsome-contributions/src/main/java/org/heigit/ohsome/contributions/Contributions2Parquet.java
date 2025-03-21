package org.heigit.ohsome.contributions;

import com.google.common.base.Stopwatch;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.contributions.minor.MinorNodeStorage;
import org.heigit.ohsome.contributions.minor.MinorWayStorage;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.rocksdb.RocksDB;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

import static org.heigit.ohsome.contributions.transformer.TransformerNodes.processNodes;
import static org.heigit.ohsome.contributions.transformer.TransformerRelations.processRelations;
import static org.heigit.ohsome.contributions.transformer.TransformerWays.processWays;

@Command(name = "contributions", aliases = {"contribs"},
        mixinStandardHelpOptions = true,
        version = "ohsome-planet contribution 1.0.0", //TODO version should be automatically set see picocli.CommandLine.IVersionProvider
        description = "generates parquet files")
public class Contributions2Parquet implements Callable<Integer> {

    @Option(names = {"--pbf"}, required = true)
    private Path pbfPath;

    @Option(names = {"--output"})
    private Path out = Path.of("out");

    @Option(names = {"--overwrite"})
    private boolean overwrite = false;

    @Option(names = {"--parallel"})
    private int parallel = Runtime.getRuntime().availableProcessors() - 1;

    @Option(names = {"--country-file"})
    private Path countryFilePath;

    @Option(names = {"--changesetDb"}, description = "full jdbc:url to changesetmd database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
    private String changesetDbUrl = "";

    @Option(names = {"--debug"}, description = "Print debug information.")
    private boolean debug = false;

    @Option(names = {"--chunks"}, description = "number of chunks which will processed. Default parallel")
    private int chunkFactor = 0;

    @Option(names = {"--include-tags"}, description = "OSM keys of relations that should be built")
    private String includeTags = "";

    public static void main(String[] args) {
        var main = new Contributions2Parquet();
        var exit = new CommandLine(main).execute(args);
        System.exit(exit);
    }

    @Override
    public Integer call() throws Exception {
        var pbf = OSMPbf.open(pbfPath);
        if (debug) {
            FileInfo.printInfo(pbf);
        }

        if (Files.exists(out)) {
            if (overwrite) {
                MoreFiles.deleteRecursively(out, RecursiveDeleteOption.ALLOW_INSECURE);
            } else {
                System.out.println("Directory already exists. To overwrite use --overwrite");
                System.exit(0);
            }
        }

        var total = Stopwatch.createStarted();

        var blobHeaders = getBlobHeaders(pbf);
        var blobTypes = pbf.blobsByType(blobHeaders);

        var tagsToInclude = includeTags.isBlank() ? List.<String>of() : Arrays.asList(includeTags.split(","));

        if (debug) {
            printBlobInfo(blobTypes);
        }

        var countryJoiner = Optional.ofNullable(countryFilePath)
                .map(SpatialJoiner::fromCSVGrid)
                .orElseGet(SpatialJoiner::noop);

        var changesetDb = Changesets.open(changesetDbUrl, parallel);

        RocksDB.loadLibrary();
        var minorNodesPath = out.resolve("minorNodes");
        processNodes(pbf, blobTypes, out, parallel, chunkFactor, minorNodesPath, countryJoiner, changesetDb);

        var minorWaysPath = out.resolve("minorWays");
        try (var minorNodes = MinorNodeStorage.inRocksMap(minorNodesPath)) {
            processWays(pbf, blobTypes, out, parallel, chunkFactor, minorNodes, minorWaysPath, x -> true, countryJoiner, changesetDb);
            try (var minorWays = MinorWayStorage.inRocksMap(minorWaysPath)) {
                processRelations(pbf, blobTypes, out, parallel, chunkFactor, minorNodes, minorWays, countryJoiner, changesetDb, tagsToInclude);
            }
        }

        System.out.println("done in " + total);
        return 0;
    }

    private void printBlobInfo(Map<OSMType, List<BlobHeader>> blobTypes) {
        System.out.println("Blobs by type:");
        System.out.println("  Nodes: " + blobTypes.get(OSMType.NODE).size() +
                " | Ways: " + blobTypes.get(OSMType.WAY).size() +
                " | Relations: " + blobTypes.get(OSMType.RELATION).size()
        );
    }

    public static List<BlobHeader> getBlobHeaders(OSMPbf pbf) {
        var blobHeaders = new ArrayList<BlobHeader>();
        try (var progress = new ProgressBarBuilder()
                .setTaskName("read blocks")
                .setInitialMax(pbf.size())
                .setUnit(" MiB", 1L << 20)
                .build()) {
            pbf.blobs().forEach(blobHeader -> {
                progress.stepTo(blobHeader.offset() + blobHeader.dataSize());
                blobHeaders.add(blobHeader);
            });
            progress.setExtraMessage(blobHeaders.size() + " blocks");
        }
        return blobHeaders;
    }
}
