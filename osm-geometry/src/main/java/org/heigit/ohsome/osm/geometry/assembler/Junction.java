package org.heigit.ohsome.osm.geometry.assembler;

import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;

public class Junction {
    private final Coordinate event;
    private final List<Arc> incomings = new ArrayList<>();
    private final List<Arc> outgoings = new ArrayList<>();

    public Junction(Coordinate event) {
        this.event = event;
    }

    public Coordinate event() {
        return event;
    }

    public boolean isEmpty() {
        return incomings.isEmpty() && outgoings.isEmpty();
    }

    public List<Arc> incomings() {
        return incomings;
    }

    public List<Arc> outgoings() {
        return outgoings;
    }
}
