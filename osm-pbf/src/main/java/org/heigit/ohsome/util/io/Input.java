package org.heigit.ohsome.util.io;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class Input {

  public static Input fromBuffer(ByteBuffer buffer) {
    return new ByteBufferInput(buffer);
  }

  public abstract byte read();

  public int readU32() {
    return Math.toIntExact(readU64());
  }

  public int readS32() {
    return decodeZigZag32(readU32());
  }

  public long readU64() {
    long result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      final byte b = read();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new RuntimeException("");
  }

  public long readS64() {
    return decodeZigZag64(readU64());
  }


  public abstract ByteBuffer readBuffer();

  public abstract String readUTF8();

  public abstract boolean hasRemaining();

  public abstract void skip(int i);

  public abstract long pos();

  public static int decodeZigZag32(final int n) {
    return (n >>> 1) ^ -(n & 1);
  }

  public static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }

  public boolean readBool() {
    return readU64() != 0L;
  }


  public static class ByteBufferInput extends Input {
    private final ByteBuffer buffer;

    public ByteBufferInput(ByteBuffer buffer) {
      this.buffer = buffer.duplicate();
    }

    @Override
    public byte read() {
      return buffer.get();
    }

    @Override
    public boolean hasRemaining() {
      return buffer.hasRemaining();
    }

    @Override
    public ByteBuffer readBuffer() {
      var length = readU32();
      var slice =  buffer.slice(buffer.position(), length);
      skip(length);
      return slice;
    }

    @Override
    public String readUTF8() {
      var length = readU32();
      var bytes = new byte[length];
      buffer.get(bytes);
      return new String(bytes, UTF_8);
    }

    @Override
    public void skip(int length) {
      buffer.position(buffer.position() + length);
    }

    @Override
    public long pos() {
      return buffer.position();
    }
  }
}
