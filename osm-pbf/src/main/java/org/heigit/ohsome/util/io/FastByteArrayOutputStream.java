package org.heigit.ohsome.util.io;

import java.io.OutputStream;
import java.util.Arrays;

public class FastByteArrayOutputStream extends OutputStream {

    public byte[] array;

    public int length;

    private int position;

    public FastByteArrayOutputStream(int size) {
        this(new byte[size]);
    }

    public FastByteArrayOutputStream(final byte[] a) {
        array = a;
    }

    public void reset() {
        length = 0;
        position = 0;
    }

    @Override
    public void write(final int b) {
        if (position >= array.length)
            array = grow(array, position + 1, length);
        array[position++] = (byte) b;
        if (length < position)
            length = position;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
        if (position + len > array.length)
            array = grow(array, position + len, position);
        System.arraycopy(b, off, array, position, len);
        if (position + len > length)
            length = position += len;
    }

    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private byte[] grow(final byte[] array, final int length, final int preserve) {
        if (length > array.length) {
            final var newLength = (int) Math.max(Math.min(2L * array.length, MAX_ARRAY_SIZE), length);

            final var t = new byte[newLength];
            System.arraycopy(array, 0, t, 0, preserve);
            return t;
        }
        return array;
    }

    public byte[] array() {
        return Arrays.copyOf(array, position);
    }
}

