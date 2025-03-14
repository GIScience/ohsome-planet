package org.heigit.ohsome.osm.pbf;

import org.heigit.ohsome.osm.OSMType;

import java.nio.ByteBuffer;

public enum GroupType {
    DENSE(OSMType.NODE), NODE(OSMType.NODE), WAY(OSMType.WAY), RELATION(OSMType.RELATION), CHANGESET(null);

    private final OSMType osmType;

    GroupType(OSMType type) {
        this.osmType = type;
    }

    public static GroupType of(ByteBuffer group) {

        return ProtoZero.messageFieldsStream(group)
                .map(field -> of(field.tag()))
                .findFirst()
                .orElseThrow();
    }

    public static GroupType of(int tag) {
        return switch (tag) {
            case 10 -> GroupType.NODE;
            case 18 -> GroupType.DENSE;
            case 26 -> GroupType.WAY;
            case 34 -> GroupType.RELATION;
            case 42 -> GroupType.CHANGESET;
            default -> throw new UnsupportedOperationException("Unsupported tag: " + tag);
        };
    }

    public OSMType osmType() {
        return osmType;
    }
}
