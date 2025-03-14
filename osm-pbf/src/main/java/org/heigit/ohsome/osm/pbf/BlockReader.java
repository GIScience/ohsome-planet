package org.heigit.ohsome.osm.pbf;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.heigit.ohsome.osm.pbf.OSMPbf.blobBuffer;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blockBuffer;

public class BlockReader {
    private BlockReader() {}

    public static Block readBlock(FileChannel ch, BlobHeader blobHeader) {
        var blockBuffer = readBlockBuffer(ch, blobHeader);
        return ProtoZero.decodeMessage(blockBuffer, Block::new);
    }

    public static ByteBuffer readBlockBuffer(FileChannel ch, BlobHeader blobHeader) {
        try {
            var blobBuffer = blobBuffer(ch, blobHeader);
            var blob = ProtoZero.decodeMessage(blobBuffer, Blob::new);
            return blockBuffer(blob);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
