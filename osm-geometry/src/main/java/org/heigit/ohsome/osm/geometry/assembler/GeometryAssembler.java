package org.heigit.ohsome.osm.geometry.assembler;

import com.google.common.collect.PeekingIterator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

import java.util.*;

import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingDouble;

public class GeometryAssembler {

    private static final Comparator<Segment> SEGMENT_ORDER =
            Comparator.comparing(Segment::start).thenComparing(Segment::end);

    private final PriorityQueue<Arc> activeArcs = new PriorityQueue<>(comparing(Arc::end).thenComparing(Arc::start));
    private final List<Arc> incomingArcs = new ArrayList<>();
    private final List<Segment> outgoingSegments = new ArrayList<>();
    private final Map<Coordinate, Junction> junctions = new HashMap<>();
    private final List<Ring> rings = new ArrayList<>();

    public Geometry assemble(Map<Long, LineString> ways, Set<Long> inner) {
        var segments = peekingIterator(extractFromWays(ways).iterator());
        while (true) {
            var incomingEvent = activeArcs.isEmpty() ? null : activeArcs.peek().end();
            var outgoingEvent = !segments.hasNext() ? null : segments.peek().start();

            if (incomingEvent == null && outgoingEvent == null) break;

            var event = incomingEvent == null ? outgoingEvent :
                    outgoingEvent == null ? incomingEvent :
                    incomingEvent.compareTo(outgoingEvent) <= 0 ? incomingEvent : outgoingEvent;

            var incoming = incoming(event);
            var outgoing = outgoing(event, segments);

            if (hasIntersection(outgoing, activeArcs)) return null; // invalid geometry
            try {
                handleEvent(event, incoming, outgoing);
            } catch (InvalidGeometryException e) {
                return null; // invalid geometry
            }
        }

        if (!junctions.isEmpty()) {
            // TODO replace later with a log and return null!
            throw new UnsupportedOperationException("Cannot assemble junctions");
        }

        if (rings.isEmpty()) {
            return null;
        }

        try {
            return MultiPolygonBuilder.build(rings);
        } catch (InvalidGeometryException e) {
            return null; // invalid geometry
        }
    }

    private void handleEvent(Coordinate event, List<Arc> incoming, List<Segment> outgoing) {
        var numberOfSegments = incomingArcs.size() + outgoingSegments.size();
        if (numberOfSegments == 0 || numberOfSegments == 1) return;

        outgoing.sort(comparingDouble(Segment::angle));
        incoming.sort(comparingDouble(Arc::endAngle).reversed());

        if (incoming.size() == 1 && outgoing.size() == 1) {
            extend(incoming.getFirst(), outgoing.getFirst());
        } else if (incoming.isEmpty() && outgoing.size() == 2) {
            startRing(event, outgoing.get(0), outgoing.get(1));
        } else if (incoming.size() == 2 && outgoing.isEmpty()) {
            merge(event, incoming.get(0), incoming.get(1));
        } else {
            touching(event, incoming, outgoing);
        }
    }

    private static class ListCursor {
        int index = 0;
    }

    private void matchingRings(Coordinate event, List<Arc> list, List<Ring> siblingRings, List<Arc> unmatched) {
        matchingRings(event, list, new ListCursor(), null, siblingRings, unmatched);
    }

    private Ring matchingRings(Coordinate event, List<Arc> list, ListCursor cursor, Arc boundary, List<Ring> siblingRings, List<Arc> unmatched) {
        while (cursor.index < list.size()) {
            var current = list.get(cursor.index);

            if (boundary != null) {
                var ring = findClosedRing(event, boundary, current);
                if (ring != null) {
                    cursor.index++;
                    return ring;
                }
            }

            if (cursor.index + 1 < list.size()) {
                var ring = findClosedRing(event, current, list.get(cursor.index + 1));
                if (ring != null) {
                    siblingRings.add(ring);
                    cursor.index += 2;
                    continue;
                }
            }

            cursor.index++;
            var nestedRings = new ArrayList<Ring>();
            var nestedUnmatched = new ArrayList<Arc>();
            var closedRing = matchingRings(event, list, cursor, current, nestedRings, nestedUnmatched);

            if (closedRing != null) {
                if (!nestedUnmatched.isEmpty()) {
                    throw new InvalidGeometryException("arc trapped inside a structure");
                }
                closedRing.holes().addAll(nestedRings);
                siblingRings.add(closedRing);
            } else {
                siblingRings.addAll(nestedRings);
                unmatched.add(current);
                unmatched.addAll(nestedUnmatched);
            }
        }
        return null;
    }

    // TODO understand better the inside function!
    private Arc inside(Coordinate event) {
        // Arcs whose last segment approaches (event.x, y>event.y) from the left
        // form a closing vertex directly above. An even number of such arcs at the
        // same endpoint is a tangent (net 0 crossings); an odd number is a crossing.
        // Track parity with merge: even count → key removed, odd count → key kept.
        // Vertical arrivals (start.x == event.x) are genuine crossings, not tangents.
        var leftApproachAbove = new HashMap<Coordinate, Arc>();
        var candidates = new ArrayList<Arc>(activeArcs.size());
        for (var arc : activeArcs) {
            var lastSegment = arc.lastSegment();
            var start = lastSegment.start();
            var end = lastSegment.end();
            // TODO we need to understand this condition better
            if (end.getX() == event.getX() && end.getY() > event.getY() && start.getX() < event.getX()) {
                leftApproachAbove.merge(end, arc, (a, b) -> null); // parity: even→absent, odd→present
            } else  if (start.getY() >= event.getY() || end.getY() >= event.getY()){
                candidates.add(arc);
            }
        }
        candidates.addAll(leftApproachAbove.values()); // odd-parity survivors are genuine crossings

        var upperArcs = 0;
        var minYAbove = Double.MAX_VALUE;
        var arcAbove = (Arc) null;
        for (var arc : candidates) {
            var y = arc.yForEvent(event);
            if (Double.isNaN(y)) continue;
            if (y > event.getY()) {
                upperArcs++;
                if (y < minYAbove) {
                    minYAbove = y;
                    arcAbove = arc;
                }
            }
        }
        if (upperArcs % 2 == 1) return arcAbove;
        return null;
    }

    private void touching(Coordinate event, List<Arc> incoming, List<Segment> outgoing) {
        incoming.forEach(arc -> arc.touching(event));

        var rootRings = new ArrayList<Ring>();
        var leftovers = new ArrayList<Arc>();
        matchingRings(event, incoming, rootRings, leftovers);

        if (leftovers.size() == 1 && outgoing.size() == 1) {
            extend(leftovers.getFirst(), outgoing.getFirst());
        } else if (leftovers.isEmpty() && outgoing.size() == 2) {
            var junction = junctions.computeIfAbsent(event, Junction::new);
            var arcs = List.of(
                    new Arc(outgoing.get(0), junction, event),
                    new Arc(outgoing.get(1), junction, event));
            junction.outgoings().addAll(arcs);
            activeArcs.addAll(arcs);
        } else if (leftovers.size() == 2 && outgoing.isEmpty()) {
            merge(event, leftovers.get(0), leftovers.get(1));
        } else if (!leftovers.isEmpty() || !outgoing.isEmpty()) {
            var junction = junctions.computeIfAbsent(event, Junction::new);
            junction.incomings().addAll(leftovers);
            for (var out : outgoing) {
                var arc = new Arc(out, junction, event);
                junction.outgoings().add(arc);
                activeArcs.add(arc);
            }
        }
        if (rootRings.isEmpty()) {
            return;
        }

        var upperArc = inside(event);
        for (var ring : rootRings) {
            if (upperArc != null) {
                rings.addAll(ring.holes());
                ring.holes().clear();
                upperArc.addHole(ring);
            } else {
                rings.add(ring);
            }
        }

    }


    private void extend(Arc arc, Segment segment) {
        arc.extend(segment);
        activeArcs.add(arc);
    }

    private void startRing(Coordinate event, Segment upper, Segment lower) {
        var junction = junctions.computeIfAbsent(event, Junction::new);
        var upperArc = new Arc(upper, junction);
        var lowerArc = new Arc(lower, junction);
        junction.outgoings().add(upperArc);
        junction.outgoings().add(lowerArc);
        activeArcs.add(upperArc);
        activeArcs.add(lowerArc);
    }

    private Ring findClosedRing(Coordinate event, Arc left, Arc right) {
        var leftJunction = left.junction();
        var rightJunction = right.junction();

        if (leftJunction == rightJunction) {
            junctions.remove(leftJunction.event());
            return new Ring(left, right);
        }

        for (var outgoing : leftJunction.outgoings()) {
            if (outgoing == left) continue;
            if (rightJunction.incomings().contains(outgoing)) {
                outgoing.appendForward(right);
                var ring = new Ring(left, outgoing);
                junctions.remove(ring.upper().junction().event());
                leftJunction.outgoings().removeIf(arc -> arc == left || arc == outgoing);
                rightJunction.outgoings().removeIf(arc -> arc == right);
                rightJunction.incomings().removeIf(arc -> arc == outgoing);
                cleanUpJunction(event, leftJunction);
                cleanUpJunction(event, rightJunction);
                return ring;
            }
        }
        return null;
    }

    private void merge(Coordinate event, Arc arc1, Arc arc2) {
        Arc left;
        Arc right;
        if (arc1.junction().event().compareTo(arc2.junction().event()) <= 0) {
            left = arc1;
            right = arc2;
        } else {
            left = arc2;
            right = arc1;
        }

        var leftJunction = left.junction();
        var rightJunction = right.junction();

        if (leftJunction == rightJunction) {
            closeRing(event, left, right);
        } else {
            left.appendReversed(right);
            rightJunction.outgoings().removeIf(arc -> arc == right);
            if (rightJunction.outgoings().size() == 1 && rightJunction.incomings().isEmpty()) {
                left.appendForward(rightJunction.outgoings().getFirst());
                activeArcs.removeIf(arc -> arc == rightJunction.outgoings().getFirst());
                activeArcs.add(left);
                junctions.remove(rightJunction.event());
            } else {
                rightJunction.incomings().add(left);
                var possibleClosedRingArc = leftJunction.outgoings()
                        .stream()
                        .filter(arc -> arc != left && arc.end().equals2D(right.start()))
                        .findAny();
                if (possibleClosedRingArc.isPresent()) {
                    var leftToRightArc = possibleClosedRingArc.get();
                    closeRing(event, left, leftToRightArc);
                    leftJunction.outgoings().removeIf(arc -> arc == left || arc == leftToRightArc);
                    rightJunction.incomings().removeIf(arc -> arc == left || arc == leftToRightArc);
                }
            }
        }
        cleanUpJunction(event, leftJunction);
        cleanUpJunction(event, rightJunction);
    }

    private void closeRing(Coordinate event, Arc left, Arc right) {
        var ring = new Ring(left, right);
        junctions.remove(ring.upper().junction().event());

        var inside = inside(event);
        if (inside != null) {
            inside.addHole(ring);
        } else {
            rings.add(ring);
        }
    }

    private void cleanUpJunction(Coordinate event, Junction junction) {
        if (junction.isEmpty()) {
            junctions.remove(junction.event());
        } else if (junction.incomings().size() == 1 && junction.outgoings().size() == 1) {
            var incoming = junction.incomings().getFirst();
            var outgoing = junction.outgoings().getFirst();
            incoming.appendForward(outgoing);
            activeArcs.removeIf(arc -> arc == outgoing);
            activeArcs.add(incoming);
            junctions.remove(junction.event());
        } else if (junction.incomings().size() == 2 && junction.outgoings().isEmpty()) {
            merge(event, junction.incomings().get(0), junction.incomings().get(1));
        }
    }


    private List<Arc> incoming(Coordinate event) {
        incomingArcs.clear();
        if (activeArcs.isEmpty() || !event.equals2D(activeArcs.peek().end())) return incomingArcs;

        while (!activeArcs.isEmpty() && event.equals2D(activeArcs.peek().end())) {
            incomingArcs.add(activeArcs.poll());
        }
        return incomingArcs;
    }

    private List<Segment> outgoing(Coordinate event, PeekingIterator<Segment> segments) {
        outgoingSegments.clear();
        while (segments.hasNext() && segments.peek().start().equals(event)) {
            var segment = segments.next();
            if (deduplicate(segments, segment)) continue;
            outgoingSegments.add(segment);
        }
        return outgoingSegments;
    }

    private static boolean deduplicate(PeekingIterator<Segment> segments, Segment segment) {
        var dups = 1;
        while (segments.hasNext() && segments.peek().equals(segment)) {
            segments.next();
            dups++;
        }
        return dups % 2 != 1;
    }

    private boolean hasIntersection(List<Segment> outgoings, PriorityQueue<Arc> active) {
        // Check 1: two outgoing segments from the same event with the same angle → collinear overlap
        for (var i = 0; i < outgoings.size(); i++) {
            for (var j = i + 1; j < outgoings.size(); j++) {
                if (outgoings.get(i).angle() == outgoings.get(j).angle()) return true;
            }
        }
        // Check 2: outgoing vs active (incoming from left), active.start() < event
        for (var out : outgoings) {
            for (var in : active) {
                if (SegmentIntersector.intersects(in.lastSegment(), out)) return true;
            }
        }
        return false;
    }

    static List<Segment> extractFromWays(Map<Long, LineString> ways) {
        var capacity = 0;
        for (var way : ways.values()) {
            capacity += way.getNumPoints() - 1;
        }
        var segments = new ArrayList<Segment>(capacity);

        for (var way : ways.entrySet()) {
            var coords = way.getValue().getCoordinates();
            for (var i = 0; i < coords.length - 1; i++) {
                segments.add(new Segment(way.getKey(), coords[i], coords[i + 1]));
            }
        }
        segments.sort(SEGMENT_ORDER);
        return segments;
    }
}
