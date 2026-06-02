package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMType;

import java.util.List;
import java.util.Map;

import static java.util.Optional.ofNullable;
import static org.heigit.ohsome.osm.OSMEntity.OSMNode;

public class ContributionsWay extends ContributionsEntity<OSMWay> {

    public ContributionsWay(List<OSMWay> osh, MemberOfFunction members) {
        super(OSMType.WAY, osh.getFirst().id(), osh, members);
    }

    public ContributionsWay(List<OSMWay> osh, Map<Long, List<OSMNode>> nodes) {
        this(osh, (type, id) -> ofNullable(nodes.get(id)).map(nodeOSH -> (Contributions) new ContributionsNode(nodeOSH)).orElseGet(() -> new EmptyContributions(type,id)));
    }

}
