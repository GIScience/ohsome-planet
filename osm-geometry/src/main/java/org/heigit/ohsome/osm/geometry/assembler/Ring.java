package org.heigit.ohsome.osm.geometry.assembler;

import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Ring {
    private final Arc upper;
    private final Arc lower;
    private final List<Ring> holes;
    private final List<Ring> sideRings = new ArrayList<>();
    private final Set<Coordinate> touching = new HashSet<>();


    public Ring(Arc arc1, Arc arc2) {
        this(arc1, arc2, new ArrayList<>());
    }

    public Ring(Arc arc1, Arc arc2,  List<Ring> holes) {
        if (arc1.endAngle() > arc2.endAngle()) {
            this.upper = arc1;
            this.lower = arc2;
        } else {
            this.upper = arc2;
            this.lower = arc1;
        }
        this.holes = holes;
        this.holes.addAll(upper.holes());
        this.sideRings.addAll(lower.holes());
        this.touching.addAll(upper.touching());
        this.touching.addAll(lower.touching());
    }

    public Arc upper() {
        return upper;
    }

    public Arc lower() {
        return lower;
    }

    public List<Ring> holes() {
        return holes;
    }

    public List<Ring> sideHoles() {
        return sideRings;
    }

    public Set<Coordinate> touching() {
        return touching;
    }


    @Override
    public String toString() {
        return "Ring: " + upper.start();
    }
}
