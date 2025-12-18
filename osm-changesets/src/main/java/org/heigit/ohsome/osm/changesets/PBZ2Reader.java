package org.heigit.ohsome.osm.changesets;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.heigit.ohsome.util.io.FastByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

public class PBZ2Reader implements Iterator<byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(PBZ2Reader.class);

    public static Flux<byte[]> read(Path path) throws IOException {
        return Flux.using(
                () -> new BufferedInputStream(Files.newInputStream(path)),
                readFromStream(path),
                PBZ2Reader::closeQuitely);
    }

    private static Function<InputStream, Flux<byte[]>> readFromStream(Path path) {
        return input -> Flux
                .generate(() -> new PBZ2Reader(input, path), PBZ2Reader::readNext)
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

    private final InputStream channel;
    private final long size;
    private final FastByteArrayOutputStream output =  new FastByteArrayOutputStream(256 << 10);

    private int count;
    private int pos;

    private byte[] next;

    public PBZ2Reader(InputStream input, Path path) throws IOException {
        this.channel = input;
        this.size= Files.size(path);


        var header = channel.readNBytes(10);
        if (header.length  != 10) {
            throw new IOException("End of Stream");
        }

        if (header[0] != 'B' || header[1] != 'Z' || header[2] != 'h') {
            throw new IOException("Stream is not in the BZip2 format");
        }

        final int blockSize = header[3];
        if (blockSize < '1' || blockSize > '9') {
            throw new IOException("BZip2 block size is invalid");
        }

        var blockSize100k = blockSize - '0';
        System.out.println("blockSize100k = " + blockSize100k);
        output.write(header);
        count = header.length;
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
        if (count >= size) {
            return null;
        }

        var bzi = 0;
        while (bzi < bzHeader.length && count < size) {
            var b = channel.read();
            count++;
            output.write(b);
            if (b == bzHeader[0]) {
                bzi = 1;
            } else if (bzi == 3 || b == bzHeader[bzi]) {
                bzi++;
            } else if (bzi > 0) {
                bzi = 0;
            }
        }

        var end =  Arrays.copyOfRange(output.array, output.size() - (bzi == 10 ? 10 : 0), output.size());
        var data = Arrays.copyOf(output.array, output.size() - (bzi == 10 ? 10 : 0));
        logger.debug("Buffer = ... %s (%d)%n", Arrays.toString(end), data.length);
        output.reset();
        output.write(end);
        return data;
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