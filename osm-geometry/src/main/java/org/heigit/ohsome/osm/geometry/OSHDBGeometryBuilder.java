package org.heigit.ohsome.osm.geometry;

import com.google.common.collect.Lists;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.*;

public class OSHDBGeometryBuilder {

    public static Geometry buildMultiPolygon(List<List<Coordinate>> outer, List<List<Coordinate>> inner) {
        var builder = new OSHDBGeometryBuilderInternal();
        return builder.getMultiPolygonGeometry(outer, inner);
    }


    private static class OSHDBGeometryBuilderInternal {
        private static final GeometryFactory geometryFactory = new GeometryFactory();

        public OSHDBGeometryBuilderInternal() {
        }

        /**
         * Construct the geometry of an OSMRelation, interpreted as a MultiPolygon geometry type,
         * given the line segments of the outer and inner rings it consists of.
         *
         * @param outerLines A list of segments which can be glued into 1 to n closed rings which form
         *                   the outer shells of the multi polygon
         * @param innerLines A list of segments which can be glued into 0 to n closed rings which form
         *                   the inner holes of the multi polygon
         * @return Geometry
         */
        public Geometry getMultiPolygonGeometry(
                List<List<Coordinate>> outerLines,
                List<List<Coordinate>> innerLines
        ) {

            // convert
            var outerL = new ArrayList<LinkedList<Coordinate>>();
            outerLines.forEach(line -> outerL.add(new LinkedList<>(line)));
            var innerL = new ArrayList<LinkedList<Coordinate>>();
            innerLines.forEach(line -> innerL.add(new LinkedList<>(line)));

            // construct inner and outer rings
            List<LinkedList<Coordinate>> outerRingsNodes = buildRings(outerL);
            List<LinkedList<Coordinate>> innerRingsNodes = buildRings(innerL);

            // check if there are any pinched off sections in outer rings
            splitPinchedRings(outerRingsNodes, innerRingsNodes);
            // check if there are any touching inner/outer rings, merge any
            mergeTouchingRings(innerRingsNodes);
            // create JTS rings for non-degenerate rings only

            List<LinearRing> outerRings = outerRingsNodes.stream()
                    .filter(ring -> ring.size() >= LinearRing.MINIMUM_VALID_SIZE)
                    .map(ring -> geometryFactory.createLinearRing(
                            ring.toArray(Coordinate[]::new)))
                    .toList();
            List<LinearRing> innerRings = innerRingsNodes.stream()
                    .filter(ring -> ring.size() >= LinearRing.MINIMUM_VALID_SIZE)
                    .map(ring -> geometryFactory.createLinearRing(
                            ring.toArray(Coordinate[]::new)))
                    .toList();

            // construct multipolygon from rings
            // todo: handle nested outers with holes (e.g. inner-in-outer-in-inner-in-outer) - worth the
            // effort? see below for a possibly much easier implementation.
            Geometry result;
            if (outerRings.size() == 1) {
                result = geometryFactory.createPolygon(
                        outerRings.getFirst(),
                        innerRings.toArray(new LinearRing[0])
                );
            } else {
                STRtree innersTree = new STRtree();
                innerRings.forEach(inner -> innersTree.insert(inner.getEnvelopeInternal(), inner));
                Polygon[] polys = outerRings.stream().map(outer -> {
                            // todo: check for inners containing other inners -> inner-in-outer-in-inner-in-outer case
                            try {
                                return constructMultipolygonPart(
                                        innersTree,
                                        geometryFactory.createPolygon(outer)
                                );
                            } catch (TopologyException e) {
                                // try again with buffer(0) on outer ring
                                Geometry buffered = geometryFactory.createPolygon(outer).buffer(0);
                                if (buffered instanceof Polygon polygon) {
                                    return constructMultipolygonPart(
                                            innersTree,
                                            polygon
                                    );
                                } else {
                                    return null;
                                }
                            }
                        })
                        .filter(Objects::nonNull)
                        .toArray(Polygon[]::new);
                // todo: what to do with unmatched inner rings??
                result = geometryFactory.createMultiPolygon(polys);
            }
            return result;
        }

        /**
         * Search and merge touching rings.
         *
         * <p>Attention: modifies the input data, such that there are no more rings that touch in
         * one or more segments.</p>
         *
         * <p>
         *   Touching rings are defined as rings which share at least one segment (a segment is formed by
         *   two consecutive ring nodes, regardless of their order). An example is:
         *   [r1 = (A,B,C,D,E,F,A); r2 = (X,Y,B,C,D,E,X)].
         *   The result would be: [r1 = (B,A,F,E,X,Y,B)] "or any equivalent representation of this ring"
         * </p>
         *
         * <pre>
         * F--E----X       F--E----X
         * |  |    |       |       |
         * |  D-C  |  -->  |       |
         * |    |  |       |       |
         * A----B--Y       A----B--Y
         * </pre>
         *
         * @param ringsNodes a collection of node-lists, each forming a ring (i.e. a closed linestring)
         */
        private static void mergeTouchingRings(Collection<LinkedList<Coordinate>> ringsNodes) {
            // ringSegments will hold a reference of which ring a particular segment is part of.
            // Note that in the final result, each segment will be "used" by exactly one ring.
            Map<Segment, LinkedList<Coordinate>> ringSegments = new HashMap<>();
            for (Iterator<LinkedList<Coordinate>> ringsIter = ringsNodes.iterator(); ringsIter.hasNext();) {
                LinkedList<Coordinate> ringNodes = ringsIter.next();
                // will contain the list of segments of the current or merged ring.
                // after the merging process, these are used to populate the ringSegments map.
                List<Segment> mergedRingSegments = new ArrayList<>(ringNodes.size() - 1);
                // pairwise iterate over nodes of current ring ->
                Iterator<Coordinate> ringNodesIter = ringNodes.iterator();
                var prevNodeId = ringNodesIter.next();
                while (ringNodesIter.hasNext()) {
                    var thisNodeId = ringNodesIter.next();
                    Segment segment = new Segment(prevNodeId, thisNodeId);
                    prevNodeId = thisNodeId;
                    if (!ringSegments.containsKey(segment)) {
                        // we have not encountered this segment yet -> just remember it for later
                        mergedRingSegments.add(segment);
                    } else {
                        // we have already seen this segment:
                        // merge this ring (ringNodes) into the previously encountered one (targetNodes)
                        LinkedList<Coordinate> targetNodes = ringSegments.get(segment);
                        // remove all segments pointing to the target ring, as we will rebuild it from scratch
                        ringSegments.values().removeAll(Collections.singleton(targetNodes));
                        // cut and rewind target and current rings to the matching segment we found
                        cutAtSegment(targetNodes, segment);
                        cutAtSegment(ringNodes, segment);
                        // cut back all other segments which are shared by current and target ring
                        mergeSegmentsToRing(targetNodes, ringNodes);
                        // clean up
                        // add merged ring's segments to segments->ring map
                        mergedRingSegments.clear();
                        Iterator<Coordinate> targetNodesIter = targetNodes.iterator();
                        var segmentPrevNodeId = targetNodesIter.next();
                        while (targetNodesIter.hasNext()) {
                            var segmentCurrNodeId = targetNodesIter.next();
                            mergedRingSegments.add(new Segment(segmentPrevNodeId, segmentCurrNodeId));
                            segmentPrevNodeId = segmentCurrNodeId;
                        }
                        // remove current ring from end result, as it was merged with another ring already.
                        ringsIter.remove();
                        // save target ring for global segments->ring map (ringSegments)
                        ringNodes = targetNodes;
                        // abort current ring, continue with next one
                        break;
                    }
                }
                // add current ring's segments to map of all already processed segments
                for (Segment mergedRingSegment : mergedRingSegments) {
                    ringSegments.put(mergedRingSegment, ringNodes);
                }
            }
        }



        /**
         * Search and split self-intersecting/pinched/figure-8 rings.
         *
         * <p>Attention: modifies the input data, such that there are no more figure-8 rings.</p>
         *
         * <p>
         *   A pinched ring forms a figure-8 configuration where the ring touches itself in a single
         *   point. An example is: [r = (A,B,C,D,E,F,C,G,A)].
         *   The result would be: [r1 = (C,D,E,G,C); r2 = (A,B,C,G,A)].
         * </p>
         *
         * <pre>
         *  A--B
         *  |  |
         *  G--C--D
         *     |  |
         *     F--E
         * </pre>
         *
         * @param ringsNodes a collection of node-lists, each forming a ring (i.e. a closed linestring)
         * @param holeRingsNodes a collection where holes formed by "upended" figure-8's should be stored
         */
        private void splitPinchedRings(
                Collection<LinkedList<Coordinate>> ringsNodes,
                Collection<LinkedList<Coordinate>> holeRingsNodes
        ) {
            Map<Coordinate, Integer> nodeIds = new HashMap<>();
            Collection<LinkedList<Coordinate>> additionalRings = new LinkedList<>();
            for (LinkedList<Coordinate> ringNodes : ringsNodes) {
                var splitRings = splitPinchedRing(ringNodes, nodeIds);
                if (splitRings != null) {
                    // if self-intersection(s) were found, we need to check whether these are next to or
                    // overlapping each other. to do this, we convert the rings to polygon geometries first
                    splitRings.add(new LinkedList<>(ringNodes));
                    ringNodes.clear();
                    var splitRingsGeoms = splitRings.stream()
                            .map(ring -> {
                                if (ring.size() >= LinearRing.MINIMUM_VALID_SIZE) {
                                    return geometryFactory.createPolygon(ring.toArray(Coordinate[]::new));
                                } else {
                                    return geometryFactory.createPolygon();
                                }
                            })
                            .toList();
                    // determine which of the rings is "coveredBy" how many of the others
                    var nestingNumbers = Collections.nCopies(splitRingsGeoms.size(), 0)
                            .toArray(new Integer [] {});
                    for (var i = 0; i < splitRingsGeoms.size(); i++) {
                        for (var j = 0; j < splitRingsGeoms.size(); j++) {
                            if (i == j) {
                                continue;
                            }
                            if (splitRingsGeoms.get(i).coveredBy(splitRingsGeoms.get(j))) {
                                nestingNumbers[i]++;
                            }
                        }
                    }
                    // sort result into (additional) rings and holes
                    for (var i = 0; i < splitRingsGeoms.size(); i++) {
                        if (nestingNumbers[i] % 2 == 0) {
                            additionalRings.add(splitRings.get(i));
                        } else {
                            holeRingsNodes.add(splitRings.get(i));
                        }
                    }
                }
            }
            ringsNodes.addAll(additionalRings);
        }

        /**
         * Search and split pinched (figure-8) rings.
         *
         * @return null if no self-intersection is found,
         *         otherwise a collection containing additional split-off rings
         */
        private static List<LinkedList<Coordinate>> splitPinchedRing(
                LinkedList<Coordinate> ringNodes,
                Map<Coordinate, Integer> nodes
        ) {
            List<LinkedList<Coordinate>> result = null;
            boolean wasSplittable;
            do {
                wasSplittable = false;
                nodes.clear();
                var currentNodePos = 0;
                for (var ringNode : ringNodes) {
                    if (nodes.containsKey(ringNode)) {
                        // split off ring between previous and current ring position
                        int nodePos = nodes.get(ringNode);
                        final var additionalRing =
                                new LinkedList<>(ringNodes.subList(nodePos, currentNodePos + 1));
                        final var remainingRing = new LinkedList<Coordinate>();
                        remainingRing.addAll(ringNodes.subList(0, nodePos));
                        remainingRing.addAll(ringNodes.subList(currentNodePos, ringNodes.size()));
                        wasSplittable = true;
                        // add to results
                        ringNodes.clear();
                        ringNodes.addAll(remainingRing);
                        if (result == null) {
                            result = new ArrayList<>();
                        }
                        result.add(additionalRing);
                        break;
                    }
                    if (currentNodePos > 0) {
                        // don't memorize start node, since it is always repeated at the end of the ring
                        nodes.put(ringNode, currentNodePos);
                    }
                    currentNodePos++;
                }
                // repeat until the ring doesn't have any more self intersections
            } while (wasSplittable);
            return result;
        }

        /**
         * Cut a ring at the given segment.
         *
         * <p>The result is stored in the input variable (modified in-place).</p>
         *
         * <p>
         *   After cutting of a ring, one gets an open line string with the ends corresponding exactly
         *   to the cut-segments nodes.
         *   Example: ring = (A,B,C,D,E,F,A); cut = (B,C); result = (C,D,E,F,A,B)
         * </p>
         *
         * <pre>
         * F--E         F--E
         * |  |         |  |
         * |  D-C  -->  |  D-C
         * |    |       |
         * A----B       A----B
         * </pre>
         *
         * @param ring a ring of nodes
         * @param cutSegment the segment where to cut at
         */
        private static void cutAtSegment(LinkedList<Coordinate> ring, Segment cutSegment) {
            // split the ring open, by removing the "redundant" coordinate.
            // example: (A,B,C,D,E,F,A) -> (B,C,D,E,F,A)
            ring.removeFirst();
            for (int i = 0; i < ring.size(); i++) {
                // do the open ends of the current ring match the cut segment?
                Segment splitSegment = new Segment(ring.getFirst(), ring.getLast());
                if (cutSegment.equals(splitSegment)) {
                    // yes -> we're done
                    return;
                } else {
                    // no -> wind the split location in the input ring one node forward
                    // example: (B,C,D,E,F,A) -> (C,D,E,F,A,B) -- split segment was (B,A) and is now (C,B)
                    ring.add(ring.removeFirst());
                }
            }
            assert false : "cut segment not found in ring";
            throw new IllegalStateException("cut segment not found in ring");
        }

        /**
         * Take two open line strings (which share a common pair of start/end nodes) and merge them into
         * a single ring without any degeneracies.
         *
         * <p>The result is stored in the target input variable (both inputs are modified in-place).</p>
         *
         * <p>
         *   After joining of a ring, one gets a closed ring with no back-tracking segments.
         *   Example: target = (B,C,D,E,F,A); source = (C,D,E,X,Y,B)
         *            result (in target) = (B,A,F,E,X,Y,B) or any equivalent representation of this ring
         * </p>
         *
         * <pre>
         * F--E       E----X       F--E----X
         * |  |       |    |       |       |
         * |  D-C  +  D-C  |  -->  |       |
         * |               |       |       |
         * A----B       B--Y       A----B--Y
         * </pre>
         *
         * @param target a ring which has been cut open using {@link #cutAtSegment(LinkedList, Segment)}
         * @param source a ring which has been cut open using {@link #cutAtSegment(LinkedList, Segment)}
         */
        private static void mergeSegmentsToRing(LinkedList<Coordinate> target, LinkedList<Coordinate> source) {
            // make sure source and target are pointing in opposite order:
            // this facilitates merging them into a closed loop in the end of this method
            if (target.getFirst().equals2D(source.getFirst())) {
                Collections.reverse(source);
            }
            // shave off shared segments between both rings
            while (source.size() > 1 && target.size() > 1
                   && source.getFirst().equals2D(target.getLast())
                   && source.get(1).equals2D(target.get(target.size() - 2))) {
                source.removeFirst();
                target.removeLast();
            }
            while (source.size() > 1 && target.size() > 1
                   && source.getLast().equals2D(target.getFirst())
                   && source.get(source.size() - 2).equals2D(target.get(1))) {
                source.removeLast();
                target.removeFirst();
            }
            // merge two halve rings to form a new complete one
            source.removeFirst();
            target.addAll(source);
        }

        private Polygon constructMultipolygonPart(
                STRtree inners,
                Polygon outer
        ) throws TopologyException {
            PreparedGeometry outerPolygon = new PreparedPolygon(outer);
            @SuppressWarnings("unchecked") // JTS returns raw types, but they are actually LinearRings
            List<LinearRing> innerCandidates = inners.query(outer.getEnvelopeInternal());
            return geometryFactory.createPolygon(
                    outer.getExteriorRing(),
                    innerCandidates.stream().filter(outerPolygon::contains).toArray(LinearRing[]::new)
            );
        }

        /**
         * Helper that joins adjacent osm ways into linear rings.
         *
         * <p>Mutates the input lists.</p>
         */
        private static List<LinkedList<Coordinate>> buildRings(
                List<LinkedList<Coordinate>> ways
        ) {
            List<LinkedList<Coordinate>> joined = new LinkedList<>();
            // iterate until there are no more ways left to process
            while (!ways.isEmpty()) {
                LinkedList<Coordinate> current = ways.removeFirst();
                if (current.isEmpty()) {
                    continue;
                }
                // iterate until the way cannot be joined to another way
                boolean joinable;
                do {
                    var first = current.getFirst();
                    var  last = current.getLast();
                    if (first.equals2D(last)) {
                        // ring is complete -> we are done
                        joined.add(current);
                        break;
                    }
                    joinable = false;
                    for (var waysIterator = ways.iterator(); waysIterator.hasNext();) {
                        LinkedList<Coordinate> what = waysIterator.next();
                        if (what.isEmpty()) {
                            continue;
                        }
                        if (last.equals2D(what.getFirst())) {
                            // end of partial ring matches to start of current line
                            what.removeFirst();
                            current.addAll(what);
                            waysIterator.remove();
                            last = current.getLast();
                            joinable = true;
                        } else if (first.equals2D(what.getLast())) {
                            // start of partial ring matches end of current line
                            what.removeLast();
                            current.addAll(0, what);
                            waysIterator.remove();
                            first = current.getFirst();
                            joinable = true;
                        } else if (last.equals2D(what.getLast())) {
                            // end of partial ring matches end of current line
                            what.removeLast();
                            current.addAll(Lists.reverse(what));
                            waysIterator.remove();
                            last = current.getLast();
                            joinable = true;
                        } else if (first.equals2D(what.getFirst())) {
                            // start of partial ring matches start of current line
                            what.removeFirst();
                            current.addAll(0, Lists.reverse(what));
                            waysIterator.remove();
                            first = current.getFirst();
                            joinable = true;
                        }
                        if (first.equals2D(last)) {
                            break;
                        }
                    }
                    // joinable==false for invalid geometries (dangling way, unclosed ring)
                } while (joinable);
            }

            return joined;
        }

        private static class Segment {
            Coordinate id1;
            Coordinate id2;

            Segment(Coordinate id1, Coordinate id2) {
                this.id1 = id1;
                this.id2 = id2;
            }

            @Override
            public boolean equals(Object other) {
                if (other instanceof Segment otherSegment) {
                    return otherSegment.id1.equals2D(this.id1) && otherSegment.id2.equals2D(this.id2)
                           || otherSegment.id1.equals2D(this.id2) && otherSegment.id2.equals2D(this.id1);
                } else {
                    return super.equals(other);
                }
            }

            @Override
            public int hashCode() {
                return Objects.hash(id1, id2);
            }
        }
    }
}
