package org.heigit.ohsome.osm.pbf;

import org.heigit.ohsome.osm.OSMType;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class OSMPbf {
    public static final int MAX_BLOB_HEADER_SIZE = 64 * 1024; // 64 kB
//    public static final int MAX_UNCOMPRESSED_BLOB_SIZE = 32 * 1024 * 1024; // 32 MB

    public static OSMPbf open(Path path) throws IOException {
        try (var ch = FileChannel.open(path)) {
            var blobHeader = blobHeader(ch);
            if (blobHeader.type() != BlobType.HEADER) {
                throw new IOException("Invalid OSMPbf header");
            }
            var blob = blob(ch, blobHeader);
            var blockBuffer = blockBuffer(blob, ByteBuffer.allocateDirect(blob.dataSize()));
            var header = ProtoZero.decodeMessage(blockBuffer, Header::new);
            return new OSMPbf(path, ch.size(), header);
        }
    }

    public static BlobHeader blobHeader(FileChannel ch) throws IOException {
        return ProtoZero.decode(blobHeaderBuffer(ch, ByteBuffer.allocateDirect(4)), new BlobHeader(ch.position()));
    }

    public static ByteBuffer blobHeaderBuffer(FileChannel ch, ByteBuffer buffer) throws IOException {
        if (ch.read(buffer.clear().limit(4)) != 4){
            throw new IOException("Invalid OSMPbf header");
        }
        var headerSize = buffer.flip().getInt();
        if (headerSize < 0 || headerSize > MAX_BLOB_HEADER_SIZE) {
            throw new IOException("Invalid OSMPbf header");
        }
        buffer = buffer.capacity() < headerSize ? ByteBuffer.allocateDirect(headerSize) : buffer;
        ch.read(buffer.clear().limit(headerSize));
        return buffer.flip();
    }

    public static ByteBuffer blobBuffer(FileChannel ch, BlobHeader blobHeader) throws IOException {
        return blobBuffer(ch, blobHeader, null);
    }
    public static ByteBuffer blobBuffer(FileChannel ch, BlobHeader blobHeader, ByteBuffer buffer) throws IOException {
        if (buffer == null || buffer.capacity() < blobHeader.dataSize()) {
            buffer = ByteBuffer.allocateDirect(blobHeader.dataSize());
        }
        ch.read(buffer.clear().limit(blobHeader.dataSize()), blobHeader.offset());
        return buffer.flip();
    }

    public static Blob blob(FileChannel ch, BlobHeader blobHeader) throws IOException {
        var buffer = blobBuffer(ch, blobHeader, ByteBuffer.allocateDirect(blobHeader.dataSize()));
        return ProtoZero.decodeMessage(buffer, Blob::new);
    }

    public static ByteBuffer blockBuffer(Blob blob) {
        return blockBuffer(blob, null);
    }

    public static ByteBuffer blockBuffer(Blob blob, ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() < blob.dataSize()) {
            buffer = ByteBuffer.allocateDirect(blob.dataSize());
        }
        buffer.clear().limit(blob.dataSize());
        return switch (blob.dataType()) {
            case ZLIB -> Blob.decompress(blob, buffer);
            case RAW -> buffer.put(blob.data()).flip();
        };
    }

    private final Path path;
    private final long size;
    private final Header header;

    private OSMPbf(Path path, long size, Header header) {
        this.path = path;
        this.size = size;
        this.header = header;
    }

    public Path path() {
        return path;
    }

    public long size() {
        return size;
    }

    public Header header() {
        return header;
    }


    public Stream<BlobHeader> blobs() {
        try {
            var ch = FileChannel.open(path, StandardOpenOption.READ);
            try {
                var length = ch.size();
                var blobPosSpliterator = new BlobPosSpliterator(ch, length);
                return StreamSupport
                        .stream(blobPosSpliterator, false)
                        .filter(blobHeader -> blobHeader.type() == BlobType.DATA)
                        .onClose(() -> closeQuietly(ch));
            } catch (Error | RuntimeException | IOException e) {
                try {
                    ch.close();
                } catch (IOException ex) {
                    try {
                        e.addSuppressed(ex);
                    } catch (Throwable ignore) {
                    }
                }
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }

    }

    public Map<OSMType, List<BlobHeader>> blobsByType(List<BlobHeader> blobs) throws IOException {
        try (var ch = FileChannel.open(path, StandardOpenOption.READ)) {
            var block = BlockReader.readBlock(ch, blobs.getFirst());
            var types = block.groupTypes();
            if (!Set.of(GroupType.DENSE, GroupType.NODE).contains(types.getFirst())) {
                throw new IOException("Expecting first block to contain NODES but got " + types);
            }
            if (types.size() > 1) {
                throw new IOException("Expecting only one block to contain NODES but got " + types);
            }

            var startWays = binarySearchInsertionPoint(blobs, 0, findStartOfType(ch, OSMType.WAY));
            var startRelations = binarySearchInsertionPoint(blobs, startWays, findStartOfType(ch, OSMType.RELATION));
            var map = new HashMap<OSMType, List<BlobHeader>>();
            map.put(OSMType.NODE, blobs.subList(0, startWays));
            map.put(OSMType.WAY, blobs.subList(startWays, startRelations));
            map.put(OSMType.RELATION, blobs.subList(startRelations, blobs.size()));
            return map;
        } catch (UncheckedIOException uio) {
            throw uio.getCause();
        }
    }

    private static ToIntFunction<BlobHeader> findStartOfType(FileChannel ch, OSMType type) {
        return blob -> {
            try {
                var block =  BlockReader.readBlock(ch, blob);
                var types = block.groupTypes();
                if (types.size() > 1) {
                    throw new IOException("Expecting only one type per block! but got " + types);
                }
                var cmp = types.getFirst().osmType().compareTo(type);
                return cmp < 0 ? -1 : 1;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static <T> int binarySearchInsertionPoint(List<T> list, int low, ToIntFunction<T> comparator) {
        var high = list.size() - 1;
        while (low <= high) {
            var mid = (low + high) >>> 1;
            var cmp = comparator.applyAsInt(list.get(mid));
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                throw new IllegalStateException();
            }
        }
        return low;
    }

    private static void closeQuietly(Closeable c) {
        try {
            c.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final class BlobPosSpliterator implements Spliterator<BlobHeader> {
        private final FileChannel ch;
        private final long limit;

        private ByteBuffer blobHeaderBuffer = ByteBuffer.allocateDirect(Integer.BYTES);

        public BlobPosSpliterator(FileChannel ch, long limit) {
            this.ch = ch;
            this.limit = limit;
        }

        @Override
        public boolean tryAdvance(Consumer<? super BlobHeader> action) {
            var blobPos = readBlobPos();
            if (blobPos == null) {
                return false;
            }
            action.accept(blobPos);
            return true;
        }

        @Override
        public Spliterator<BlobHeader> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.DISTINCT;
        }

        private BlobHeader readBlobPos() {
            try {
                if (ch.position() >= limit) {
                    return null;
                }
                blobHeaderBuffer = blobHeaderBuffer(ch, blobHeaderBuffer);
                var blobHeader = ProtoZero.decode(blobHeaderBuffer, new BlobHeader(ch.position()));
                ch.position(blobHeader.offset() + blobHeader.dataSize());
                return blobHeader;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
