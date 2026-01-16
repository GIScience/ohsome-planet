package org.heigit.ohsome.contributions;

import com.google.common.collect.Streams;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.OSMPbf;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.heigit.ohsome.contributions.util.Utils.getBlobHeaders;

public class FileInfo {


    public static void printInfo(Path path) throws IOException {
        var pbf = OSMPbf.open(path);
        printInfo(pbf);

        var blobHeaders = getBlobHeaders(pbf);
        var blobTypes = pbf.blobsByType(blobHeaders);
        printBlobInfo(blobTypes);
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

    private static void printBlobInfo(Map<OSMType, List<BlobHeader>> blobTypes) {
        System.out.println("Blobs by type:");
        System.out.println("  Nodes: " + blobTypes.get(OSMType.NODE).size() +
                           " | Ways: " + blobTypes.get(OSMType.WAY).size() +
                           " | Relations: " + blobTypes.get(OSMType.RELATION).size()
        );
    }
}
