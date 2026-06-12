package org.heigit.ohsome.osm.geometry.assembler;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.locationtech.jts.geom.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

public class MultiPolygonBuilder {
    private static final GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(1e7));

    public static MultiPolygon build(List<Ring> rings) {
        var polygons = rings.stream()
                .mapMulti(MultiPolygonBuilder::toPolygon)
                .toArray(Polygon[]::new);
        if (polygons.length == 0) {
            return null;
        }
        return FACTORY.createMultiPolygon(polygons);
    }

    private static void toPolygon(Ring ring, Consumer<Polygon> consumer) {
        var allRings = new ArrayList<Ring>(ring.holes().size() + 1);
        allRings.add(ring);
        allRings.addAll(ring.holes());
        if(!checkTouchingPairs(allRings)){
            // throw new InvalidGeometryException("holes form a touching cycle, disconnecting interior");
            return;
        }
        var holes = new ArrayList<Ring>();
        for(var hole : ring.holes()) {
            if (!ring.envelope().covers(hole.envelope())) {
                ringToPolygon(hole, hole.holes(), consumer);
            } else {
                holes.add(hole);
            }
        }
        ringToPolygon(ring, holes, consumer);
    }

    private static void ringToPolygon(Ring ring, List<Ring> ringHoles, Consumer<Polygon> consumer) {
        var shell = toLinearRing(ring);
        var holes = ringHoles.stream()
//                .<Ring>mapMulti((r, downstream) -> {
//                    downstream.accept(r);
//                    ringHoles.forEach(rh -> ringToPolygon(rh, rh.holes(), consumer));
//                })
                .map(MultiPolygonBuilder::toLinearRing)
                .toArray(LinearRing[]::new);
        consumer.accept(FACTORY.createPolygon(shell, holes));
    }

    // Bipartite graph: holes (0..n-1) and touching coordinates (n..n+m-1).
    // A cycle means holes collectively disconnect the polygon interior.
    // All holes sharing ONE point form a star (tree) — valid.
    // Holes forming a loop via DISTINCT points form a cycle — invalid.
    private static boolean checkTouchingPairs(List<Ring> rings) throws InvalidGeometryException {
        var touching = rings.stream().filter(r -> !r.touching().isEmpty()).toList();
        if (touching.size() < 2) return true;
        var coordIds = new HashMap<Coordinate, Integer>();
        for (var ring : touching) {
            for (var c : ring.touching()) {
                coordIds.computeIfAbsent(c, x -> touching.size() + coordIds.size());
            }
        }
        var parent = new int[touching.size() + coordIds.size()];
        for (var i = 0; i < parent.length; i++) parent[i] = i;
        for (var i = 0; i < touching.size(); i++) {
            for (var c : touching.get(i).touching()) {
                var ri = find(parent, i);
                var rc = find(parent, coordIds.get(c));
                if (ri == rc) return false;
                parent[ri] = rc;
            }
        }
        return true;
    }

    private static int find(int[] parent, int i) {
        while (parent[i] != i) i = parent[i];
        return i;
    }

    /**
     * Build a closed coordinate ring from a Ring's upper and lower HalfArcs.
     * The upper arc runs left→right along the top boundary; the lower arc runs
     * left→right along the bottom boundary.  The polygon ring is:
     *   upper[0..n] → lower[m-2..0]   (lower reversed, start already covered by upper's end being lower's end)
     * which closes because lower[0] == upper[0] == the shared start coordinate.
     */
    private static LinearRing toLinearRing(Ring ring) {
        var upper = ring.upper().coordinates();
        if (ring.lower() == null) {
            return FACTORY.createLinearRing(upper.toArray(new Coordinate[0]));
        }
        var lower = ring.lower().coordinates();
        var coords = new ArrayList<Coordinate>(upper.size() + lower.size() - 1);


        var itr = Iterators.peekingIterator(Iterators.concat(upper.iterator(), Lists.reverse(lower).iterator()));
        var coord = itr.next();
        coords.add(coord);
        while(itr.hasNext()) {
            var c = itr.next();
            if (coord.equals2D(c)) {
                continue;
            }

            coords.add(c);
            coord = c;
        }
        return FACTORY.createLinearRing(coords.toArray(new Coordinate[0]));
    }
}
