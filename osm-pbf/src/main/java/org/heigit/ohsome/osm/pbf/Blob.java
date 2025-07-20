package org.heigit.ohsome.osm.pbf;

import org.heigit.ohsome.util.io.Input;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class Blob implements ProtoZero.Message {

    public enum DataType {
        RAW, ZLIB
    }

    private int dataSize;
    private DataType dataType;
    private ByteBuffer data;

    @Override
    public boolean decode(Input input, int tag) {
        switch (tag) {
            case 10 -> data(DataType.RAW, input.readBuffer());
            case 16 -> dataSize(input.readU32());
            case 26 -> data(DataType.ZLIB, input.readBuffer());
            case 34, 42, 50, 58 -> throw new UnsupportedOperationException(); // lzma, bzip2, lz4, zstd
            default -> {
                // log skipped tags!
                return false;
            }
        }
        return true;
    }

    public int dataSize() {
        return  dataSize;
    }

    public void dataSize(int size) {
        this.dataSize = size;
    }

    public DataType dataType() {
        return dataType;
    }

    public ByteBuffer data() {
        if (data != null) {
            return data.duplicate();
        }
        return null;
    }

    public void data(DataType type, ByteBuffer data) {
        this.dataType = type;
        this.data = data;
    }

    public static ByteBuffer decompress(Blob blob, ByteBuffer buffer) {
        var inflater = new Inflater();
        inflater.setInput(blob.data());
        try {
            inflater.inflate(buffer);
            inflater.end();
            return buffer.flip();
        } catch (DataFormatException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }

    @Override
    public String toString() {
        return "Blob{" +
               "dataSize=" + dataSize +
               ", dataType=" + dataType +
               ", data=" + data +
               '}';
    }
}
