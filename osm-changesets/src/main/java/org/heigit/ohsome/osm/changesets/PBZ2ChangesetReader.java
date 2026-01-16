package org.heigit.ohsome.osm.changesets;

import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static com.google.common.primitives.Bytes.concat;
import static com.google.common.primitives.Bytes.indexOf;
import static java.util.Arrays.copyOfRange;

public class PBZ2ChangesetReader {
    private static final byte[] OSM_OPEN = "<osm>\n".getBytes();
    private static final byte[] OSM_CLOSE = "</osm>".getBytes();
    private static final byte[] CHANGESET_OPEN = " <changeset id=\"".getBytes();

    public static Flux<byte[]> read(Path path) throws IOException {
        return PBZ2Reader.read(path)
                .window(2, 1)
                .concatMap(Flux::collectList)
                .map(PBZ2ChangesetReader::completeBlocksForChangesets);
    }

    private static byte[] completeBlocksForChangesets(List<byte[]> blocks) {
        var first = alignStartChangeset(blocks.getFirst());
        if (blocks.size() == 1) {
            // last block in whole file, just needs an osm open tag!
            return concat(OSM_OPEN, first);
        }
        return concat(OSM_OPEN, first, completeChangeset(blocks.getLast()), OSM_CLOSE);
    }

    private static byte[] alignStartChangeset(byte[] block) {
        return copyOfRange(block, indexOf(block, CHANGESET_OPEN), block.length);
    }

    private static byte[] completeChangeset(byte[] block) {
        return Arrays.copyOf(block, indexOf(block, CHANGESET_OPEN));
    }
}
