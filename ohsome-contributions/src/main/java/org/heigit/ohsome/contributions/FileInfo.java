package org.heigit.ohsome.contributions;

import com.google.common.collect.Streams;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.Callable;

@Command(name = "fileinfo",
        description = "print header for osm pbf file")
public class FileInfo implements Callable<Integer> {
    @Option(names = {"--pbf"}, required = true)
    private Path path;

    @Override
    public Integer call() throws Exception {
        var pbf = OSMPbf.open(path);
        printInfo(pbf);
        return 0;
    }

    public static void printInfo(OSMPbf pbf) {
        var header = pbf.header();
        System.out.printf("""
                File:
                  Name: %s
                  Size: %d%n""", pbf.path(), pbf.size());
        System.out.printf("""
                        Header:
                          Bounding_Boxes: %s
                          History: %b
                          Generator: %s
                          Replication:
                            Base_Url: %s
                            Sequence_Number: %d
                            Timestamp: %s
                          Features:%n""",
                header.bbox(),
                header.withHistory(),
                header.writingProgram(),
                header.replicationBaseUrl(),
                header.replicationSequenceNumber(),
                Instant.ofEpochSecond(header.replicationTimestamp()));
        Streams.concat(header.requiredFeatures().stream(), header.optionalFeatures().stream())
                .forEach(feature -> System.out.println("  - " + feature));

    }
}
