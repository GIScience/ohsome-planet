package org.heigit.ohsome.osm.pbf;

import org.heigit.ohsome.util.io.Input;

public class BlobHeader implements ProtoZero.Message {

    private final long offset;
    private BlobType type;
    private int dataSize;

    public BlobHeader(long offset) {
        this.offset = offset;
    }


    @Override
    public boolean decode(Input input, int tag) {
        switch (tag) {
            case 10 -> this.type = BlobType.of(input.readUTF8());
            case 18 -> {
                // skip indexData!
                return false;
            }
            case 24 -> this.dataSize = input.readU32();
            default -> {
                // log skipped tags!
                return false;
            }
        }
        return true;
    }

    public long offset() {
        return offset;
    }

    public BlobType type() {
        return type;
    }

    public int dataSize() {
        return dataSize;
    }

    @Override
    public String toString() {
        return "BlobHeader{" +
               "type=" + type +
               ", offset=" + offset +
               ", dataSize=" + dataSize +
               '}';
    }
}
