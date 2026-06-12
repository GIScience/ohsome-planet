package org.heigit.ohsome.osm.geometry.assembler;

import org.locationtech.jts.geom.Coordinate;

class SegmentIntersector {

    // Precondition: a.start() < b.start()
    // Known ordering: as < bs (precondition), as < ae, bs < be (canonical segments)
    static boolean intersects(Segment a, Segment b) {
        var as = a.start();
        var ae = a.end();
        var bs = b.start();
        var be = b.end();

        // Quick y-range check: no overlap → no intersection
        if (Math.max(as.y, ae.y) < Math.min(bs.y, be.y)) return false;
        if (Math.min(as.y, ae.y) > Math.max(bs.y, be.y)) return false;

        // Which side of line a are b's endpoints on?
        var sideOfBs = orient(as, ae, bs);
        var sideOfBe = orient(as, ae, be);

        if (sideOfBs != 0 && sideOfBe != 0) {
            // Both off the line — same side means no intersection
            if (sideOfBs * sideOfBe > 0) return false;
            // b straddles line a — check if a also reaches line b
            var sideOfAs = orient(bs, be, as);
            var sideOfAe = orient(bs, be, ae);
            if (sideOfAs * sideOfAe < 0) return true; // proper crossing
            // a.end exactly on segment b? (a.start can't be on b due to precondition)
            return sideOfAe == 0 && bs.compareTo(ae) < 0 && ae.compareTo(be) < 0;
        }

        // b.start collinear with a: as < bs already, so on segment a iff bs < ae
        if (sideOfBs == 0 && bs.compareTo(ae) < 0) return true;

        // b.end collinear with a: as < bs < be, so on segment a iff be < ae
        if (sideOfBe == 0 && be.compareTo(ae) < 0) return true;

        // a.end collinear with b: on segment b iff bs < ae < be
        // (a.start < b.start so a.start can never lie on segment b — skip it)
        var sideOfAe = orient(bs, be, ae);
        return sideOfAe == 0 && bs.compareTo(ae) < 0 && ae.compareTo(be) < 0;
    }

    // Signed area of triangle (p, q, r); sign encodes orientation
    private static double orient(Coordinate p, Coordinate q, Coordinate r) {
        return (q.x - p.x) * (r.y - p.y) - (q.y - p.y) * (r.x - p.x);
    }
}
