package org.heigit.ohsome.osm;

public record OSMMember(OSMId osmId, String role) {
    public OSMMember(OSMType type, long id, String role) {
        this(new OSMId(type, id), role);
    }

    // todo: for ways only?
    public OSMMember(long id) {
        this(new OSMId(OSMType.NODE, id), "");
    }

    public OSMType type(){
        return osmId.type();
    }

    public long id(){
        return osmId.id();
    }
}
