package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMId;
import org.heigit.ohsome.osm.OSMType;

import java.util.*;

public class ContributionsRelation extends ContributionsEntity<OSMRelation> {

    public ContributionsRelation(List<OSMRelation> osh, MemberOfFunction members) {
        super(OSMType.RELATION, osh.getFirst().id(), osh, members);
    }

    public ContributionsRelation(List<OSMRelation> osh, Map<OSMId, Contributions> oshMembers) {
        this(osh, (type, id) -> oshMembers.get(new OSMId(type, id)));
    }
}
