package org.heigit.ohsome.util.io;

public class Output extends FastByteArrayOutputStream {

    private final byte[] buf = new byte[10];

    public Output(int size) {
        super(size);
    }


    public void writeSInt32(int val) {
        writeU32(encodeZigZag32(val));
    }

    public void writeU32(int val) {
        var p = 0;
        while (true) {
            if ((val & ~0x7F) == 0) {
                buf[p++] = (byte) val;
                break;
            } else {
                buf[p++] = (byte) ((val & 0x7F) | 0x80);
                val >>>= 7;
            }
        }
        write(buf, 0, p);
    }

    public void writeS64(long val) {
        writeU64(encodeZigZag64(val));
    }

    public void writeU64(long val) {
        var p = 0;
        while (true) {
            if ((val & ~0x7FL) == 0) {
                buf[p++] = (byte) val;
                break;
            } else {
                buf[p++] = (byte) (((int) val & 0x7F) | 0x80);
                val >>>= 7;
            }
        }
        write(buf, 0, p);
    }

    public static int encodeZigZag32(final int n) {
        return (n << 1) ^ (n >> 31);
    }

    public static long encodeZigZag64(final long n) {
        return (n << 1) ^ (n >> 63);
    }
}
