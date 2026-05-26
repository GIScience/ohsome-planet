package org.heigit.ohsome.osm.geometry.assembler;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.*;

public class Arc {
    private final List<Coordinate> coordinates = new ArrayList<>();
    private final Envelope envelope;
    private final Junction junction;
    private final Set<Long> wayIds = new HashSet<>();
    private final Segment firstSegment;
    private final List<Ring> holes = new ArrayList<>();
    private Segment lastSegment;

    private final List<Coordinate> touching = new ArrayList<>();

    public Arc(Segment segment, Junction junction, Coordinate touching) {
        this(segment, junction);
        this.touching(touching);
    }

    public Arc(Segment segment, Junction junction) {
        this.coordinates.add(segment.start());
        this.coordinates.add(segment.end());
        this.envelope = new Envelope(segment.start(), segment.end());

        this.junction = junction;

        this.wayIds.add(segment.wayId());

        this.firstSegment = segment;
        this.lastSegment = segment;
    }

    public List<Coordinate> coordinates() {
        return coordinates;
    }

    public Envelope envelope() {
        return envelope;
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
        this.envelope.expandToInclude(segment.end());
    }

    public double yForEvent(Coordinate event) {
        var coord = coordinates.getLast();
        for (var i= coordinates.size() - 1; i >= 0; i--) {
            var coordinate = coordinates.get(i);
            if (coordinate.getX() == event.getX() && coordinate.getY() < event.getY()) {
                return coordinate.getY();
            }

            if (coordinate.equals2D(event)) {
                if (i > 0) return coordinates.get(i-1).getY();
                continue;
            }

            if (i == 0 || coordinate.getX() < event.getX()) {
                return getYForX(coordinate, coord, event.getX());
            }
            coord = coordinate;
        }
        return Double.NaN;
    }

    public static double getYForX(Coordinate a, Coordinate b, double x) {
        // Safety check: Avoid division by zero if the line is perfectly vertical
        if (Double.compare(a.x, b.x) == 0) {
            if (Double.compare(x, a.x) == 0) {
                return a.y; // x is exactly on the vertical line; return an endpoint Y
            }
            return Math.max(a.y, b.y);
//            throw new IllegalArgumentException("The line is vertical. An infinite number of Y values exist for X = " + x);
        }

        // 1. Find the ratio: how far is 'x' along the distance from a.x to b.x
        double t = (x - a.x) / (b.x - a.x);

        // 2. Apply that same ratio to interpolate the Y value
        return a.y + t * (b.y - a.y);
    }

    public void appendForward(Arc other) {
        this.coordinates.addAll(other.coordinates.subList(1, other.coordinates.size()));
        this.lastSegment = other.lastSegment;
        this.wayIds.addAll(other.wayIds);
        this.holes.addAll(other.holes);
        this.touching.addAll(other.touching);
        this.envelope.expandToInclude(other.envelope);
    }

    public void appendReversed(Arc other) {
        this.coordinates.addAll(other.coordinates.subList(0, other.coordinates.size() - 1).reversed());
        this.lastSegment = other.firstSegment;
        this.wayIds.addAll(other.wayIds);
        this.holes.addAll(other.holes);
        this.touching.addAll(other.touching);
        this.envelope.expandToInclude(other.envelope);
    }

    public void addHole(Ring ring) {
        this.holes.add(ring);
        this.holes.addAll(ring.sideHoles());
        ring.sideHoles().clear();
    }

    public void touching(Coordinate event) {
        if (event != null) {
            this.touching.add(event);
        }
    }

    public List<Coordinate> touching() {
        return touching;
    }

    public List<Ring> holes() {
        return holes;
    }

    @Override
    public String toString() {
        return coordinates.stream().map(c -> c.getX() + " " + c.getY()).collect(joining(", ", "LINESTRING (", ")"));
    }
}
