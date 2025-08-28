package org.heigit.ohsome.osm.changesets;

import com.google.common.primitives.Bytes;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

public class PBZ2Reader implements Iterator<byte[]> {

    public static Flux<byte[]> read(Path path) throws IOException {
        return Flux.using(
                () -> Files.newByteChannel(path),
                PBZ2Reader::readFromStream,
                PBZ2Reader::closeQuitely);
    }

    private static Flux<byte[]> readFromStream(SeekableByteChannel input) {
        return Flux
                .generate(() -> new PBZ2Reader(input), PBZ2Reader::readNext)
                .flatMapSequential(block -> fromCallable(() -> decompress(block)).subscribeOn(parallel()));
    }

    private static void closeQuitely(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
//                throw new UncheckedIOException(e);
        }
    }

    public static byte[] decompress(byte[] block) throws IOException {
        return (new BZip2CompressorInputStream(new ByteArrayInputStream(block), true)).readAllBytes();
    }

    private static final byte[] bzHeader = {'B', 'Z', 'h', '9', 0x31, 0x41, 0x59, 0x26, 0x53, 0x59};

    private final SeekableByteChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    private final byte[] array = buffer.array();
    private final byte[] data = new byte[256 << 10];

    private int count;
    private int limit;
    private int pos;
    private int bzi;

    private byte[] next;

    public PBZ2Reader(SeekableByteChannel channel) throws IOException {
        this.channel = channel;
        limit = channel.read(buffer);
        if (Bytes.indexOf(array, bzHeader) != 0) {
            throw new IOException("missing bzip header at the beginning!");
        }
        System.arraycopy(bzHeader, 0, data, 0, bzHeader.length);
        count = bzHeader.length;
        pos = count;
    }

    @Override
    public boolean hasNext() {
        try {
            return next != null || (next = computeNext()) != null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        var ret = next;
        next = null;
        return ret;
    }


    private byte[] computeNext() throws IOException {
        while (true) {
            if (pos >= limit) {
                if (channel.position() >= channel.size()) {
                    return null;
                }
                limit = channel.read(buffer.clear());
                pos = 0;
            }
            var offset = pos;
            while (bzi < bzHeader.length && pos < limit) {
                var b = array[pos++];
                if (b == bzHeader[bzi]) {
                    bzi++;
                } else if (b == bzHeader[0]) {
                    bzi = 1;
                } else {
                    bzi = 0;
                }
            }
            var len = pos - offset;
            //TODO ensure capacity
            System.arraycopy(array, offset, data, count, len);
            count += len;
            if (bzi == bzHeader.length) {
                bzi = 0;
                var ret = new byte[count - 10];
                System.arraycopy(data, 0, ret, 0, ret.length);
                count = bzHeader.length;
                return ret;
            }
            if (channel.position() >= channel.size()) {
                var ret = new byte[count];
                System.arraycopy(data, 0, ret, 0, ret.length);
                return ret;
            }
        }
    }

    public PBZ2Reader readNext(SynchronousSink<byte[]> sink) {
        try {
            if (!hasNext()) {
                sink.complete();
            }
            var n = next();
            sink.next(n);
        } catch (Exception e) {
            sink.error(e);
        }
        return this;
    }

}