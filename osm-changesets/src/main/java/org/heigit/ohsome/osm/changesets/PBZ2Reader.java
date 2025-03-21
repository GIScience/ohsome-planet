package org.heigit.ohsome.osm.changesets;

import com.google.common.primitives.Bytes;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PBZ2Reader implements Iterator<byte[]> {
    private static final byte[] bzHeader = {'B', 'Z', 'h', '9', 0x31, 0x41, 0x59, 0x26, 0x53, 0x59};

    private final SeekableByteChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    private final byte[] array = buffer.array();
    private final byte[] data = new byte[256 << 10];

    private int count = 0;
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
                bzi = b == bzHeader[bzi] ? bzi + 1 :
                        b == bzHeader[0] ? 1 : 0;
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
            var data = next();
            sink.next(data);
        } catch (Exception e) {
            sink.error(e);
        }
        return this;
    }

}
