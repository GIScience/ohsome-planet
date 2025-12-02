package org.heigit.ohsome.replication.update;

import com.google.common.collect.Iterators;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.contributions.contrib.*;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMMember;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.replication.UpdateStore;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toMap;
import static org.heigit.ohsome.contributions.util.Utils.fetchChangesets;
import static org.heigit.ohsome.osm.OSMType.NODE;
import static org.heigit.ohsome.osm.OSMType.WAY;
import static org.heigit.ohsome.replication.UpdateStore.BackRefs.*;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

public class ContributionUpdater {
    public record Entity<T extends OSMEntity>(long id, List<T> newVersions, T before) {

        public List<T> osh() {
            var osh = new ArrayList<T>(newVersions.size() + 1);
            if (before != null) {
                osh.add(before);
            }
            osh.addAll(newVersions);
            return osh;
        }
    }

    private final Changesets changesetDb;
    private final SpatialJoiner countryJoiner;
    private final UpdateStore store;

    private Map<Long, Entity<OSMNode>> newNodes;
    private Map<Long, Entity<OSMWay>> newWays;
    private Map<Long, Entity<OSMRelation>> newRelations;

    private final Map<Long, OSMWay> updatedWays = new ConcurrentHashMap<>();
    private final Map<Long, OSMRelation> updatedRelations = new ConcurrentHashMap<>();


    public ContributionUpdater(UpdateStore store, Changesets changesetDb, SpatialJoiner countryJoiner) {
        this.store = store;
        this.changesetDb = changesetDb;
        this.countryJoiner = countryJoiner;
    }

    public Flux<Contrib> update(List<OSMEntity> osc) {
        return update(osc.iterator());
    }

    public Flux<Contrib> update(Iterator<OSMEntity> osc) {
        var itr = Iterators.peekingIterator(osc);
        return Flux.concat(
                Flux.just(itr).concatMap(this::updateNodes),
                Flux.just(itr).concatMap(this::updateWays),
                Flux.just(itr).concatMap(this::updateRelations));
    }


    public void updateStore() {
        store.nodes(newNodes.values().stream()
                .map(Entity::newVersions)
                .map(List::getLast)
                .collect(toMap(OSMEntity::id, identity())));

        store.ways(updatedWays);
        store.relations(updatedRelations);

        updateNodeWayBackRefs();
        updateTypeRelationBackRefs();
    }

    private record BackRefsUpdate(Set<Long> exist, Set<Long> toRemove) {
        public BackRefsUpdate() {
            this(new HashSet<>(), new HashSet<>());
        }
    }

    private void updateNodeWayBackRefs() {
        var nodeWayBackRefsUpdate = new HashMap<Long, BackRefsUpdate>();

        newWays.entrySet().stream()
                .filter(not(entry -> entry.getValue().newVersions().isEmpty()))
                .forEach(entry -> {
                    var wayId = entry.getKey();
                    var newVersions = entry.getValue().newVersions();
                    var last = newVersions.getLast();
                    var before = Optional.ofNullable(entry.getValue().before());
                    var refs = new HashSet<>(last.refs());
                    before.map(OSMWay::refs).ifPresent(oldRefs -> oldRefs.stream()
                            .filter(not(refs::contains))
                            .forEach(refToRemove -> nodeWayBackRefsUpdate.computeIfAbsent(refToRemove, x -> new BackRefsUpdate()).toRemove().add(wayId))
                    );
                    refs.forEach(refToExists -> nodeWayBackRefsUpdate.computeIfAbsent(refToExists, x -> new BackRefsUpdate()).exist().add(wayId));
                });

        var refIds = new HashSet<>(nodeWayBackRefsUpdate.keySet());
        var nodeWayBackRefs = new HashMap<>(store.backRefs(NODE_WAY, refIds));
        for (var nodeId : refIds) {
            var wayBackRefs = nodeWayBackRefs.computeIfAbsent(nodeId, x -> new HashSet<>());
            var update = nodeWayBackRefsUpdate.get(nodeId);
            wayBackRefs.addAll(update.exist());
            wayBackRefs.removeAll(update.toRemove());
        }
        store.backRefs(NODE_WAY, nodeWayBackRefs);
    }

    private void updateTypeRelationBackRefs() {
        var backRefsUpdate = Map.of(
                NODE, new HashMap<Long, BackRefsUpdate>(),
                WAY, new HashMap<Long, BackRefsUpdate>());

        newRelations.entrySet().stream()
                .filter(not(entry -> entry.getValue().newVersions().isEmpty()))
                .forEach(entry -> {
                    var relId = entry.getKey();
                    var last = entry.getValue().newVersions().getLast();
                    var before = Optional.ofNullable(entry.getValue().before());
                    var memberRefs = Map.of(
                            NODE, new HashSet<Long>(),
                            WAY, new HashSet<Long>());
                    last.members().stream()
                            .filter(not(member -> member.type().equals(OSMType.RELATION)))
                            .forEach(member -> memberRefs.get(member.type()).add(member.id()));

                    before.map(OSMRelation::members).ifPresent(beforeMembers -> beforeMembers.stream()
                            .filter(not(member -> member.type().equals(OSMType.RELATION)))
                            .filter(not(member -> memberRefs.get(member.type()).contains(member.id())))
                            .forEach(memberToRemove -> backRefsUpdate.get(memberToRemove.type()).computeIfAbsent(memberToRemove.id(), x -> new BackRefsUpdate()).toRemove().add(relId))
                    );
                    memberRefs.forEach((type, refs) -> refs.forEach(refId -> backRefsUpdate.get(type).computeIfAbsent(refId, x -> new BackRefsUpdate()).exist().add(relId)));
                });

        var nodeRelationBackRefs = new HashMap<>(store.backRefs(NODE_RELATION, backRefsUpdate.get(NODE).keySet()));
        updateTypeBackRefs(backRefsUpdate.get(NODE), nodeRelationBackRefs);
        store.backRefs(NODE_RELATION, nodeRelationBackRefs);

        var wayRelationBackRefs = new HashMap<>(store.backRefs(WAY_RELATION, backRefsUpdate.get(WAY).keySet()));
        updateTypeBackRefs(backRefsUpdate.get(WAY), wayRelationBackRefs);
        store.backRefs(WAY_RELATION, wayRelationBackRefs);
    }

    private void updateTypeBackRefs(Map<Long, BackRefsUpdate> refIdBackRefUpdates, Map<Long, Set<Long>> typeRelationBackRefs) {
        refIdBackRefUpdates.forEach((refId, update) -> {
            var backRefs = typeRelationBackRefs.computeIfAbsent(refId, x -> new HashSet<>());
            backRefs.addAll(update.exist());
            backRefs.removeAll(update.toRemove());
        });
    }

    public Flux<Contrib> updateNodes(Iterator<OSMEntity> osc) {
        newNodes = newNodes(osc);
        return Flux.fromIterable(newNodes.entrySet())
                .flatMapSequential(entity -> fromCallable(() -> updateNode(entity.getKey(), entity.getValue().newVersions(), entity.getValue().before())).subscribeOn(parallel()))
                .flatMapIterable(identity());
    }

    public Flux<Contrib> updateWays(Iterator<OSMEntity> osc) {
        newWays = newWays(osc);
        return Flux.fromIterable(newWays.values())
                .flatMapSequential(entity -> fromCallable(() -> updateWay(entity)).subscribeOn(parallel()))
                .flatMapIterable(identity());
    }

    public Flux<Contrib> updateRelations(Iterator<OSMEntity> osc) {
        newRelations = newRelations(osc);
        return Flux.fromIterable(newRelations.values())
                .flatMapSequential(entity -> fromCallable(() -> updateRelation(entity)).subscribeOn(parallel()))
                .flatMapIterable(identity());
    }


    private List<Contrib> updateNode(long nodeId,List<OSMNode> newVersions, OSMNode before) throws Exception {
        var osh = new ArrayList<OSMNode>(newVersions.size() + 1);
        if (before != null) {
            osh.add(before);
        }
        osh.addAll(newVersions);

        var changesetIds = osh.stream().map(OSMEntity::changeset).collect(Collectors.toSet());
        var changesets = fetchChangesets(changesetIds, changesetDb);

        var contributions = new ContributionsNode(osh);
        return getContribs(contributions, before, changesets);
    }

    private List<Contrib> updateWay(Entity<OSMWay> entity) throws Exception {
        var osh = entity.osh();

        var refIds = new HashSet<Long>();
        osh.forEach(osm -> refIds.addAll(osm.refs()));

        var nodes = store.nodes(refIds).values().stream()
                .collect(Collectors.toMap(OSMNode::id, List::of));

        updateMembers(refIds, nodes, newNodes);


        var changesetIds = new HashSet<Long>();
        collectChangesetIds(nodes, changesetIds);
        osh.forEach(osm -> changesetIds.add(osm.changeset()));
        var changesets = fetchChangesets(changesetIds, changesetDb);

        var contributions = new ContributionsWay(osh, nodes);
        var contribs = getContribs(contributions, entity.before(), changesets);

        var last = contribs.getLast();
        var osm = osh.getLast();
        var minorVersion = last.getOsmMinorVersion();
        var edits = last.getOsmEdits();
        updatedWays.put(osm.id(), osm.withMinorAndEdits(minorVersion, edits));

        return contribs;
    }

    private List<Contrib> updateRelation(Entity<OSMRelation> entity) throws Exception {
        var osh = entity.osh();

        var nodeIds = new HashSet<Long>();
        var wayIds = new HashSet<Long>();
        osh.stream()
                .map(OSMRelation::members)
                .<OSMMember>mapMulti(Iterable::forEach)
                .forEach(member -> {
                    switch (member.type()) {
                        case NODE -> nodeIds.add(member.id());
                        case WAY -> wayIds.add(member.id());
                        default -> {}
                    }
                });


        var ways = store.ways(wayIds).values().stream()
                .collect(Collectors.toMap(OSMEntity::id, List::of));
        updateMembers(wayIds, ways, newWays);

        ways.values().stream().<OSMWay>mapMulti(Iterable::forEach)
                .map(OSMWay::refs)
                .forEach(nodeIds::addAll);

        var nodes = store.nodes(nodeIds).values().stream()
                .collect(Collectors.toMap(OSMEntity::id, List::of));
        updateMembers(nodeIds, nodes, newNodes);


        var changesetIds = new HashSet<Long>();

        collectChangesetIds(nodes, changesetIds);
        collectChangesetIds(ways, changesetIds);
        osh.forEach(osm -> changesetIds.add(osm.changeset()));
        var changesets = fetchChangesets(changesetIds, changesetDb);

        var contributions = new ContributionsRelation(osh, Contributions.memberOf(nodes, ways));
        var contribs = getContribs(contributions, entity.before(), changesets);

        var last = contribs.getLast();
        var osm = osh.getLast();
        var minorVersion = last.getOsmMinorVersion();
        var edits = last.getOsmEdits();
        updatedRelations.put(osm.id(), osm.withMinorAndEdits(minorVersion, edits));

        return contribs;
    }


    private <T extends OSMEntity> void updateMembers(Set<Long> ids, Map<Long, List<T>> members, Map<Long, Entity<T>> newVersions) {
        ids.stream()
                .map(newVersions::get)
                .filter(Objects::nonNull)
                .forEach(entity -> members.put(entity.id(), entity.osh()));
    }

    private <T extends OSMEntity> void collectChangesetIds(Map<Long, List<T>> members, Set<Long> changesets) {
        members.values().stream()
                .<OSMEntity>mapMulti(Iterable::forEach)
                .map(OSMEntity::changeset)
                .forEach(changesets::add);
    }


    private <T extends OSMEntity> ArrayList<Contrib> getContribs(Contributions contributions, T before, Map<Long, ContribChangeset> changesets) {
        var converter = new ContributionsAvroConverter(contributions, changesets::get, countryJoiner);
        if (before != null) {
            converter.setMinorAndEdits(before.minorVersion(), before.edits() - 1);
        }

        var updates = new ArrayList<Contrib>();
        while (converter.hasNext()) {
            var contrib = converter.next();
            if (contrib.isEmpty()) {
                continue;
            }
            var con = contrib.get();
            if (before != null && before.version() == con.getOsmVersion()) {
                con.setOsmMinorVersion(con.getOsmMinorVersion() + before.minorVersion());
            }
            if (con.getChangeset().getId() != -1) {
                updates.add(con);
            }

        }
        return updates;
    }


    public <T extends OSMEntity> Map<Long, List<T>> getByType(Iterator<OSMEntity> osc, Class<T> clazz) {
        var itr = Iterators.peekingIterator(osc);
        var newVersions = new HashMap<Long, List<T>>();
        while (itr.hasNext() && clazz.isInstance(itr.peek())) {
            var id = itr.peek().id();
            var osh = new ArrayList<T>(2);
            while (itr.hasNext() && clazz.isInstance(itr.peek()) && itr.peek().id() == id) {
                //noinspection unchecked
                osh.add((T) itr.next());
            }
            newVersions.put(id, osh);
        }
        return newVersions;

    }

    private <T extends OSMEntity> SortedMap<Long, Entity<T>> filter(Map<Long, List<T>> newVersions, Map<Long, T> versionBefore) {
        var filtered = new TreeMap<Long, Entity<T>>();
        newVersions.forEach((id, osh) -> {
            var before = versionBefore.get(id);
            if (osh.isEmpty()) {
                // minor edits
                filtered.put(id, new Entity<>(id, osh, before));
                return;
            }
            if (before != null) {
                osh.removeIf(version -> version.version() <= before.version());
            } else {
                osh.removeIf(not(OSMEntity::visible));
            }

            if (!osh.isEmpty()) {
                filtered.put(id, new Entity<>(id, osh, before));
            }
        });
        return filtered;
    }

    public Map<Long, Entity<OSMNode>> newNodes(Iterator<OSMEntity> osc) {
        var newVersions = getByType(osc, OSMNode.class);

        var versionBefore = new HashMap<>(store.nodes(newVersions.keySet()));
        return filter(newVersions, versionBefore);
    }

    public Map<Long, Entity<OSMWay>> newWays(Iterator<OSMEntity> osc) {
        var newVersions = getByType(osc, OSMWay.class);
        var nodeWaysBackRefs = store.backRefs(NODE_WAY, newNodes.keySet());
        nodeWaysBackRefs.forEach((nodeId, ways) -> {
            for (var wayId : ways) {
                newVersions.computeIfAbsent(wayId, x -> List.of());
            }
        });

        var versionBefore = new HashMap<>(store.ways(newVersions.keySet()));
        return filter(newVersions, versionBefore);
    }

    public Map<Long, Entity<OSMRelation>> newRelations(Iterator<OSMEntity> osc) {
        var newVersions = getByType(osc, OSMRelation.class);
        var nodeRelsBackRefs = store.backRefs(NODE_RELATION, newNodes.keySet());
        nodeRelsBackRefs.forEach((nodeId, relations) -> relations.forEach(relId -> newVersions.computeIfAbsent(relId, x -> List.of())));

        var wayRelsBackRefs = store.backRefs(WAY_RELATION, newWays.keySet());
        wayRelsBackRefs.forEach((wayId, relations) -> relations.forEach(relId -> newVersions.computeIfAbsent(relId, x -> List.of())));

        var versionBefore = new HashMap<>(store.relations(newVersions.keySet()));
        return filter(newVersions, versionBefore);
    }

}
