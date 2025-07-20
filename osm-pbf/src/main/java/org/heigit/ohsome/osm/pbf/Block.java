package org.heigit.ohsome.osm.pbf;

import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.pbf.ProtoZero.Field;
import org.heigit.ohsome.osm.pbf.ProtoZero.Message;
import org.heigit.ohsome.osm.pbf.group.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;

import static org.heigit.ohsome.osm.pbf.ProtoZero.messageFieldsIterator;

public class Block implements Message {

    private final List<ByteBuffer> groups = new ArrayList<>();
    private ByteBuffer stringTable;
    private List<String> strings;
    private long granularity = 100;
    private long dateGranularity = 1000;
    private long lonOffset = 0;
    private long latOffset = 0;

    @Override
    public boolean decode(Input input, int tag) {
        switch (tag) {
            case 10 -> stringTable = input.readBuffer();
            case 18 -> groups.add(input.readBuffer());
            case 136 -> granularity = input.readU32();
            case 144 -> dateGranularity = input.readU32();
            case 152 -> latOffset = input.readU64();
            case 160 -> lonOffset = input.readU64();
            default -> {
                return false;
            }
        }
        return true;
    }

    public List<String> strings() {
        if (strings == null) {
            strings = new ArrayList<>();
            parseStringTable(stringTable);
        }
        return strings;
    }

    public Stream<OSMEntity> entities() {
        return groups().stream()
                .<Field>mapMulti((g, downstream) -> messageFieldsIterator(g).forEachRemaining(downstream))
                .mapMulti((field, downstream) -> entities(field).forEach(downstream));

    }

    private Group<? extends  OSMEntity> entities(Field field) {
        if (!(field instanceof ProtoZero.LenField(var tag, var buffer))) {
            throw new UnsupportedOperationException(String.format("Field %s is not a ProtoZero.LenField", field));
        }

        return switch (GroupType.of(tag)) {
            case DENSE -> ProtoZero.decode(buffer, new GroupDense(this));
            case NODE -> ProtoZero.decode(buffer, new GroupNode(this));
            case WAY -> ProtoZero.decode(buffer, new GroupWay(this));
            case RELATION -> ProtoZero.decode(buffer, new GroupRelation(this));
            case CHANGESET -> throw new UnsupportedOperationException("Unsupported GroupType CHANGESET");
        };
    }

    private void parseStringTable(ByteBuffer buffer) {
        ProtoZero.decode(buffer, (input, tag) -> {
            if (tag != 10) {
                return false;
            }
            strings.add(input.readUTF8());
            return true;
        });
    }

    public List<ByteBuffer> groups() {
        return groups;
    }

    public List<GroupType> groupTypes() {
        return groups().stream().map(GroupType::of).distinct().toList();
    }

    public double parseLat(long degree) {
        return (granularity * degree + latOffset) / 1E9;
    }

    /**
     * Convert a longitude value stored in a protobuf into a double, compensating for granularity and longitude offset
     */
    public double parseLon(long degree) {
        return (granularity * degree + lonOffset) / 1E9;
    }

    @Override
    public String toString() {
        return "Block {" +
               "strings=" + strings().size() +
               ", groups=" + groups().size() +
               ", granularity=" + granularity +
               ", dateGranularity=" + dateGranularity +
               ", lonOffset=" + lonOffset +
               ", latOffset=" + latOffset +
               '}';
    }

    public String string(int idx) {
        return strings().get(idx);
    }

    public long parseTimestamp(long timestamp) {
        return timestamp * dateGranularity;
    }
}
