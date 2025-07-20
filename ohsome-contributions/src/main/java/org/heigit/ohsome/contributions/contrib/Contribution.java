package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.heigit.ohsome.osm.OSMEntity.*;

public record Contribution(Instant timestamp, long changeset, int userId, String user, OSMEntity entity,
                           List<ContribMember> members, Map<String, Object> data) {
    public Contribution(Instant timestamp, long changeset, int userId, String user, OSMEntity entity, List<ContribMember> members) {
        this(timestamp, changeset, userId, user, entity, members, new ConcurrentHashMap<>());
    }

    public Contribution(OSMNode osmNode) {
        this(osmNode.timestamp(), osmNode.changeset(), osmNode.userId(), osmNode.user(), osmNode, emptyList(), new ConcurrentHashMap<>());
    }

    @SuppressWarnings("unchecked")
    public <T> T data(String key, Function<Contribution, T> supplier) {
        return (T) data.computeIfAbsent(key, x -> supplier.apply(this));
    }

    public record ContribMember(OSMType type, long id, Contribution contrib, String role) {
    }
}
