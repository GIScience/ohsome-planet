package org.heigit.ohsome.osm.geometry.assembler;

import org.locationtech.jts.geom.Coordinate;

import java.util.Objects;

public class Segment {
    private final long wayId;
    private final Coordinate start;
    private final Coordinate end;

    private boolean hasAngle;
    private double angle;

    public Segment(long wayId, Coordinate start, Coordinate end) {
        this.wayId = wayId;
        if (start.compareTo(end) > 0) {
            this.start = end;
            this.end = start;
        } else {
            this.start = start;
            this.end = end;
        }
    }

    public long wayId() {
        return wayId;
    }

    public double angle() {
        if (!hasAngle) {
            hasAngle = true;
            angle = Math.atan2(end.x - start.x, end.y - start.y);
        }
        return angle;
    }

    public Coordinate start() {
        return start;
    }

    public Coordinate end() {
        return end;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Segment) obj;
        return Objects.equals(this.start, that.start) &&
               Objects.equals(this.end, that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }
}
