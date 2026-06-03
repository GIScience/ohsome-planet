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
        try {
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
                handleEvent(event, incoming, outgoing);
            }

            if (!junctions.isEmpty()) {
                for (var event : junctions.keySet()) {
                    var junction = junctions.get(event);
                    if (junction == null || junction.incomings().isEmpty()) {
                        continue;
                    }
                    handleEvent(junction.event(), junction.incomings(), List.of());
                }
            }

            if (!rings.isEmpty()) {
                return MultiPolygonBuilder.build(rings);
            }
        } catch (InvalidGeometryException e) {
            // fall through
        }
        return null; // invalid geometry
    }

    private void handleEvent(Coordinate event, List<Arc> incoming, List<Segment> outgoing) {
        var numberOfSegments = incomingArcs.size() + outgoingSegments.size();
        if (numberOfSegments == 0 || numberOfSegments == 1) {
//            throw new InvalidGeometryException("unclosed ends detected!");
            if (!incoming.isEmpty()) {
                var arc = incoming.getFirst();
                var junction = arc.junction();
                junction.outgoings().removeIf(a -> a == arc);
                junction.incomings().removeIf(a -> a == arc);
                if (junction.outgoings().size() == 1) {
                    activeArcs.removeIf(a -> a == junction.outgoings().getFirst());
                    junction.outgoings().clear();
                }
                cleanUpJunction(junction);
            }
            return;
        }

        if (outgoing.size() > 1) outgoing.sort(comparingDouble(Segment::angle));

        if (incoming.size() > 1) incoming.sort(comparingDouble(Arc::endAngle).reversed());


        var touching = numberOfSegments > 2 ? event : null;

        incoming.forEach(arc -> arc.touching(touching));

        var rootRings = new ArrayList<Ring>();
        var leftovers = (List<Arc>) new ArrayList<Arc>();
        if (incoming.size() > 1) {
            matchingRings(incoming, rootRings, leftovers);
        } else {
            leftovers = incoming;
        }

        if (leftovers.size() == 1 && outgoing.size() == 1) {
            extend(leftovers.getFirst(), outgoing.getFirst());
        } else if (leftovers.isEmpty() && outgoing.size() == 2) {
            startRing(event, outgoing.get(0), outgoing.get(1), touching);
        } else if (leftovers.size() == 2 && outgoing.isEmpty()) {
            merge(leftovers.get(0), leftovers.get(1));
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
        if (upperArc == null) {
            rings.addAll(rootRings);
        } else {
            for (var ring : rootRings) {
                upperArc.addHole(ring);
                ring.holes().forEach(upperArc::addHole);
                ring.holes().clear();
            }
        }
    }

    private static class ListCursor {
        int index = 0;
    }

    private void matchingRings(List<Arc> list, List<Ring> siblingRings, List<Arc> unmatched) {
        matchingRings(list, new ListCursor(), null, siblingRings, unmatched);
    }

    private Ring matchingRings(List<Arc> list, ListCursor cursor, Arc boundary, List<Ring> siblingRings, List<Arc> unmatched) {
        while (cursor.index < list.size()) {
            var current = list.get(cursor.index);

            if (boundary != null) {
                var ring = findClosedRing(boundary, current);
                if (ring != null) {
                    cursor.index++;
                    return ring;
                }
            }

            if (cursor.index + 1 < list.size()) {
                var ring = findClosedRing(current, list.get(cursor.index + 1));
                if (ring != null) {
                    siblingRings.add(ring);
                    cursor.index += 2;
                    continue;
                }
            }

            cursor.index++;
            var nestedRings = new ArrayList<Ring>();
            var nestedUnmatched = new ArrayList<Arc>();
            var closedRing = matchingRings(list, cursor, current, nestedRings, nestedUnmatched);

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

    private Arc inside(Coordinate event) {
        var upperArcs = 0;
        var minYAbove = Double.MAX_VALUE;
        var arcAbove = activeArcs.peek();
        for (var arc : activeArcs) {
            if (event.equals(arc.start())) continue;
            var y = arc.yForEvent(event);
            if (Double.isNaN(y)) continue;
            if (y > event.getY()) {
                upperArcs++;
                if (y < minYAbove || (y == minYAbove && arcAbove.endAngle() > arc.endAngle())) {
                    minYAbove = y;
                    arcAbove = arc;
                }
            }
        }
        if (upperArcs % 2 == 1) return arcAbove;
        return null;
    }


    private void extend(Arc arc, Segment segment) {
        arc.extend(segment);
        activeArcs.add(arc);
    }

    private void startRing(Coordinate event, Segment upper, Segment lower, Coordinate touching) {
        var junction = junctions.computeIfAbsent(event, Junction::new);
        var upperArc = new Arc(upper, junction, touching);
        var lowerArc = new Arc(lower, junction, touching);
        junction.outgoings().add(upperArc);
        junction.outgoings().add(lowerArc);
        activeArcs.add(upperArc);
        activeArcs.add(lowerArc);
    }

    private Ring findClosedRing(Arc arc1, Arc arc2) {
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
            leftJunction.outgoings().removeIf(arc -> arc == left || arc == right);
            cleanUpJunction(leftJunction);
            return new Ring(left, right);
        }

        for (var outgoing : leftJunction.outgoings()) {
            if (outgoing == left) continue;
            if (rightJunction.incomings().contains(outgoing)) {
                outgoing.appendForward(right);
                var ring = new Ring(left, outgoing);
                var ringJunction = ring.upper().junction();
                ringJunction.incomings().removeIf(arc -> arc == left || arc == outgoing);
                ringJunction.outgoings().removeIf(arc -> arc == right || arc == outgoing);
                cleanUpJunction(ringJunction);
                leftJunction.outgoings().removeIf(arc -> arc == left || arc == outgoing);
                rightJunction.outgoings().removeIf(arc -> arc == right);
                rightJunction.incomings().removeIf(arc -> arc == outgoing);
                cleanUpJunction(leftJunction);
                cleanUpJunction(rightJunction);
                return ring;
            }
        }
        return null;
    }

    private void merge(Arc arc1, Arc arc2) {
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

        left.appendReversed(right);
        rightJunction.outgoings().removeIf(arc -> arc == right);
        if (rightJunction.outgoings().size() == 1 && rightJunction.incomings().isEmpty()) {
            left.appendForward(rightJunction.outgoings().getFirst());
            activeArcs.removeIf(arc -> arc == rightJunction.outgoings().getFirst());
            activeArcs.add(left);
        } else {
            rightJunction.incomings().add(left);
        }
        cleanUpJunction(leftJunction);
        cleanUpJunction(rightJunction);
    }

    private void cleanUpJunction(Junction junction) {
        if (junction.isEmpty()) {
            junctions.remove(junction.event());
        } else if (junction.incomings().size() == 1 && junction.outgoings().size() == 1) {
            var incoming = junction.incomings().getFirst();
            var outgoing = junction.outgoings().getFirst();
            incoming.appendForward(outgoing);
            var outgoingJunction = junctions.get(outgoing.end());
            if (outgoingJunction != null) {
                if (outgoingJunction.outgoings().removeIf(arc -> arc == outgoing)) {
                    outgoingJunction.outgoings().add(incoming);
                }
                if (outgoingJunction.incomings().removeIf(arc -> arc == outgoing)) {
                    outgoingJunction.incomings().add(incoming);
                }
            }
            if (activeArcs.removeIf(arc -> arc == outgoing)) {
                activeArcs.add(incoming);
            }
            junctions.remove(junction.event());
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
