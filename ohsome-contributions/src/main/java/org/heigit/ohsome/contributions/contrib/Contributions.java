package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public interface Contributions extends Iterator<Contribution> {

    static ContributionsEntity.MemberOfFunction memberOf(Map<Long, List<OSMEntity.OSMNode>> nodes, Map<Long, List<OSMEntity.OSMWay>> ways) {
        return (type, id) -> (switch (type) {
            case NODE -> ofNullable(nodes.get(id)).map(osh -> (Contributions) new ContributionsNode(osh));
            case WAY -> ofNullable(ways.get(id)).map(osh -> (Contributions) new ContributionsWay(osh, nodes));
            default -> Optional.<Contributions>empty();
        }).orElseGet(() -> new EmptyContributions(type, id));
    }

    @Override
    boolean hasNext();

    @Override
    Contribution next();

    Contribution peek();

    Contribution prev();

    long id();

    OSMType type();
}
