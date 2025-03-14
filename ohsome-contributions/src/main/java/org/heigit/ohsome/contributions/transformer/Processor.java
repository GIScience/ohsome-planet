package org.heigit.ohsome.contributions.transformer;

import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.OSMPbf;

import java.nio.channels.FileChannel;
import java.util.List;

public record Processor(int id, OSMPbf pbf, FileChannel ch, List<BlobHeader> blobs, int offset, int limit) {
    public boolean isWithHistory() {
        return pbf.header().withHistory();
    }
}
