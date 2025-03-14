package org.heigit.ohsome.osm.pbf;

public enum BlobType {
    HEADER("OSMHeader"), DATA("OSMData");

    private final String type;

    BlobType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }

    public static BlobType of(String type) {
        return switch (type) {
            case "OSMHeader" -> HEADER;
            case "OSMData" -> DATA;
            default -> throw new IllegalArgumentException("invalid BlobType " + type);
        };
    }
}
