package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMId;
import org.heigit.ohsome.osm.OSMType;

import java.util.*;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public abstract class Contributions implements Iterator<Contribution> {
    private static class EmptyContributions extends Contributions {
        public EmptyContributions(OSMId osmId) {
            super(osmId);
        }

        @Override
        protected Contribution computeNext() {
            return null;
        }
    }

    public static Contributions empty(OSMId osmId) {
        return new EmptyContributions(osmId);
    }

    public static Function<List<OSMNode>, Contributions> node() {
        return ContributionsNode::new;
    }

    public static Function<List<OSMWay>, Contributions> way(Map<Long, List<OSMNode>> nodes) {
        return osh -> new ContributionsWay(osh, nodes);
    }

    public static Function<List<OSMRelation>, Contributions> relation(Map<Long, List<OSMNode>> nodes, Map<Long, List<OSMWay>> ways) {
        return osh -> new ContributionsRelation(osh, Contributions.memberOf(nodes, ways));

    }

    public static Function<OSMId,Contributions> memberOf(Map<Long, List<OSMNode>> nodes, Map<Long, List<OSMWay>> ways) {
        return osmId -> (switch (osmId.type()) {
            case NODE -> ofNullable(nodes.get(osmId.id())).map(Contributions.node());
            case WAY -> ofNullable(ways.get(osmId.id())).map(Contributions.way(nodes));
            default -> Optional.<Contributions>empty();
        }).orElseGet(() -> Contributions.empty(osmId));

    }


    protected Contribution prev;
    private Contribution next;

    private final OSMId osmId;
    public Contributions(OSMId osmId) {
        this.osmId = osmId;
    }

    @Override
    public boolean hasNext() {
        return next != null || (next = computeNext()) != null;
    }

    @Override
    public Contribution next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        prev = next;
        next = null;
        return prev;
    }

    public Contribution peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next;
    }

    public Contribution prev() {
        return prev;
    }

    public OSMId osmId() {
        return osmId;
    }

    public long id() {
        return osmId.id();
    }

    public OSMType type() {
        return osmId.type();
    }

    protected abstract Contribution computeNext();
}
