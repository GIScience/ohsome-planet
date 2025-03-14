package org.heigit.ohsome.osm.pbf;

import org.heigit.ohsome.util.io.Input;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ProtoZero {

    public interface Message {

        default void startDecode() {}

        boolean decode(Input input, int tag);

        default void finish() {}
    }

    public enum WireType {
        VARINT(0), //	int32, int64, uint32, uint64, sint32, sint64, bool, enum
        I64(1), //	fixed64, sfixed64, double
        LEN(2), //	string, bytes, embedded messages, packed repeated fields
        SGROUP(3), //	group start (deprecated)
        EGROUP(4), //	group end (deprecated)
        I32(5); //	fixed32, sfixed32, float

        private final int type;

        WireType(int type) {
            this.type = type;
        }

        public int tag(int field) {
            return field << 3 | type;
        }

        public static int field(int tag) {
            return tag >> 3;
        }

        public static WireType type(int tag) {
            var type = tag & 0x7;
            if (type >= WireType.values().length) {
                throw new IllegalArgumentException("tag has no valid type! [" + type + "]");
            }
            return WireType.values()[type];
        }
    }


    public static <R extends Message> R decode(ByteBuffer buffer, R message) {
        return decode(Input.fromBuffer(buffer), message);
    }

    public static <R extends Message> R decodeMessage(ByteBuffer buffer, Supplier<R> message) {
        return decode(Input.fromBuffer(buffer), message.get());
    }


    public static <R extends Message> R decode(Input input, R message) {
        while (input.hasRemaining()) {
            var tag = input.readU32();
            if (tag == 0) {
                break;
            }
            if (!message.decode(input, tag)) {
                skip(input, tag);
            }
        }
        message.finish();
        return message;
    }

    public static Stream<Field> messageFieldsStream(ByteBuffer buffer) {
        return messageFieldsStream(Input.fromBuffer(buffer));
    }

    public static Stream<Field> messageFieldsStream(Input input) {
        return StreamSupport.stream(new MessageFieldsIterator(input), false);
    }

    public static Iterator<Field> messageFieldsIterator(ByteBuffer buffer) {
        return messageFieldsIterator(Input.fromBuffer(buffer));
    }

    public static Iterator<Field> messageFieldsIterator(Input input) {
        return new MessageFieldsIterator(input);
    }

    private static void skip(Input input, int tag) {
        var type = WireType.type(tag);
        switch (type) {
            case VARINT -> input.readU64();
            case I64 -> input.skip(8);
            case LEN -> input.skip(input.readU32());
            case SGROUP, EGROUP -> throw new UnsupportedOperationException("deprecated types " + type +" tag " + tag);
            case I32 -> input.skip(4);
        }
    }

    // https://protobuf.dev/programming-guides/encoding/#structure
    public sealed interface Field  permits VarIntField, LenField {
        int tag();
    }

    public record VarIntField(int tag, long value) implements Field {

    }
    public record LenField(int tag, ByteBuffer buffer) implements Field {

    }

    public static class MessageFieldsIterator implements Iterator<Field>, Spliterator<Field> {
        private final Input input;
        private Field next;

        private MessageFieldsIterator(Input input) {
            this.input = input;
        }

        @Override
        public boolean hasNext() {
            return (next != null) || (next = computeNext()) != null;
        }

        @Override
        public Field next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            var ret = next;
            next = null;
            return ret;
        }

        @Override
        public void forEachRemaining(Consumer<? super Field> action) {
            do { } while (tryAdvance(action));
        }

        @Override
        public boolean tryAdvance(Consumer<? super Field> action) {
            var field = computeNext();
            if (field == null) {
                return false;
            }
            action.accept(field);
            return true;
        }

        @Override
        public Spliterator<Field> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL;
        }

        private Field computeNext() {
            var tag = 0;
            if (!input.hasRemaining() || (tag = input.readU32()) == 0) {
                return null;
            }
            var type = WireType.type(tag);
            return switch (type) {
                case VARINT -> new VarIntField(tag, input.readU64());
                case LEN -> new LenField(tag, input.readBuffer());
                case I32, I64, SGROUP, EGROUP -> throw new UnsupportedOperationException("deprecated types " + type);
            };
        }
    }
}
