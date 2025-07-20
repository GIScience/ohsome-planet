package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMId;
import org.heigit.ohsome.osm.OSMType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public interface Contributions extends Iterator<Contribution> {

    static Function<OSMId, Contributions> memberOf(Map<Long, List<OSMEntity.OSMNode>> nodes, Map<Long, List<OSMEntity.OSMWay>> ways) {
        return osmId -> (switch (osmId.type()) {
            case NODE -> ofNullable(nodes.get(osmId.id())).map(id -> (Contributions) new ContributionsNode(id));
            case WAY -> ofNullable(ways.get(osmId.id())).map(id -> (Contributions) new ContributionsWay(id, nodes));
            default -> Optional.<Contributions>empty();
        }).orElseGet(() -> new EmptyContributions(osmId));
    }

    @Override
    boolean hasNext();

    @Override
    Contribution next();

    Contribution peek();

    Contribution prev();

    OSMId osmId();

    long id();

    OSMType type();
}
