package org.heigit.ohsome.osm;

public record OSMMember(OSMType type, long id, String role) {

    public OSMMember(OSMId osmId, String role) {
        this(osmId.type(), osmId.id(), role);

    }
    // todo: for ways only?
    public OSMMember(long id) {
        this(new OSMId(OSMType.NODE, id), "");
    }

    public OSMType type(){
        return type;
    }

    public long id(){
        return id;
    }

}
