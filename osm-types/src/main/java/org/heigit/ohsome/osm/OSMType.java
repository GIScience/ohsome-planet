package org.heigit.ohsome.osm;

public enum OSMType {
    NODE(0),
    WAY(1),
    RELATION(2);

    private final int id;
    private final String toString;

    OSMType(int id) {
        this.id = id;
        this.toString = name().toLowerCase();
    }

    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return toString;
    }


    public static OSMType parseType(int id) {
        return switch (id) {
            case 0 -> NODE;
            case 1 -> WAY;
            case 2 -> RELATION;
            default -> throw new IllegalArgumentException();
        };
    }

    public static OSMType parseType(String type) {
        return switch (type.trim().toLowerCase()) {
            case "node" -> NODE;
            case "way" -> WAY;
            case "relation" -> RELATION;
            default -> throw new IllegalArgumentException();
        };
    }
}
