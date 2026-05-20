package org.heigit.ohsome.osm.geometry.assembler;

import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Arc {
    private final List<Coordinate> coordinates = new ArrayList<>();
    private final Junction junction;
    private final Set<Long> wayIds = new HashSet<>();
    private final Segment firstSegment;
    private final List<Ring> holes = new ArrayList<>();
    private Segment lastSegment;

    private final List<Coordinate> touching = new ArrayList<>();

    public Arc(Segment segment, Junction junction, Coordinate touching) {
        this(segment, junction);
        this.touching.add(touching);
    }

    public Arc(Segment segment, Junction junction) {
        this.coordinates.add(segment.start());
        this.coordinates.add(segment.end());

        this.junction = junction;

        this.wayIds.add(segment.wayId());

        this.firstSegment = segment;
        this.lastSegment = segment;
    }

    public List<Coordinate> coordinates() {
        return coordinates;
    }

    public Coordinate start() {
        return coordinates.getFirst();
    }

    public Coordinate end() {
        return coordinates.getLast();
    }

    public Segment lastSegment() {
        return lastSegment;
    }


    public double endAngle() {
        return lastSegment.angle();
    }

    public Junction  junction() {
        return junction;
    }

    public void extend(Segment segment) {
        this.coordinates.add(segment.end());
        this.lastSegment = segment;
        this.wayIds.add(segment.wayId());
    }

    public double yForEvent(Coordinate event) {
        var coord = coordinates.getLast();
        for (var i= coordinates.size() - 1; i >= 0; i--) {
            var coordinate = coordinates.get(i);
            if (coordinate.getX() == event.getX() && coordinate.getY() < event.getY()) {
                return coordinate.getY();
            }
            if ( i == 0 && coordinate.equals2D(event)) {
                return Double.NaN;
            }
            if (i == 0 || coordinate.getX() < event.getX()) {
                return Math.max(coordinate.getY(), coord.getY());
            }
            coord = coordinate;
        }
        return Double.NaN;
    }

    public void appendForward(Arc other) {
        this.coordinates.addAll(other.coordinates.subList(1, other.coordinates.size()));
        this.lastSegment = other.lastSegment;
        this.wayIds.addAll(other.wayIds);
        this.holes.addAll(other.holes);
        this.touching.addAll(other.touching);
    }

    public void appendReversed(Arc other) {
        this.coordinates.addAll(other.coordinates.subList(0, other.coordinates.size() - 1).reversed());
        this.lastSegment = other.firstSegment;
        this.wayIds.addAll(other.wayIds);
        this.holes.addAll(other.holes);
        this.touching.addAll(other.touching);
    }

    public void addHole(Ring ring) {
        this.holes.add(ring);
    }

    public void touching(Coordinate event) {
        this.touching.add(event);
    }

    public List<Coordinate> touching() {
        return touching;
    }

    public List<Ring> holes() {
        return holes;
    }

}
