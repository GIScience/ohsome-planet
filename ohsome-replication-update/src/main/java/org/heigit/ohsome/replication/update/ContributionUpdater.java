package org.heigit.ohsome.replication.update;

import com.google.common.collect.Iterators;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.contributions.contrib.Contributions;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsNode;
import org.heigit.ohsome.contributions.contrib.ContributionsWay;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
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
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

public class ContributionUpdater {
    public record Entity<T extends OSMEntity>(List<T> newVersions, T before) {
    }

    private final Changesets changesetDb;
    private final SpatialJoiner countryJoiner;
    private final UpdateStore store;

    private Map<Long, Entity<OSMNode>> newNodes;
    private Map<Long, Entity<OSMWay>> newWays;

    private final Map<Long, OSMWay> updatedWays = new ConcurrentHashMap<>();


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

        updateNodeWayBackRefs();
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
        var nodeWayBackRefs = new HashMap<>(store.backRefsNodeWay(refIds));
        for (var nodeId : refIds) {
            var wayBackRefs = nodeWayBackRefs.computeIfAbsent(nodeId, x -> new HashSet<>());
            var update = nodeWayBackRefsUpdate.get(nodeId);
            wayBackRefs.addAll(update.exist());
            wayBackRefs.removeAll(update.toRemove());
        }
        store.backRefsNodeWay(nodeWayBackRefs);
    }

    public Flux<Contrib> updateNodes(Iterator<OSMEntity> osc) {
        newNodes = newNodes(osc);
        return Flux.fromIterable(newNodes.entrySet())
                .flatMapSequential(entity -> fromCallable(() -> updateNode(entity.getKey(), entity.getValue().newVersions(), entity.getValue().before())).subscribeOn(parallel()))
                .flatMapIterable(identity());
    }

    public Flux<Contrib> updateWays(Iterator<OSMEntity> osc) {
        newWays = newWays(osc);
        return Flux.fromIterable(newWays.entrySet())
                .flatMapSequential(entity -> fromCallable(() -> updateWay(entity.getKey(), entity.getValue().newVersions(), entity.getValue().before())).subscribeOn(parallel()))
                .flatMapIterable(identity());
    }

    public Flux<Contrib> updateRelations(Iterator<OSMEntity> osc) {

        return Flux.empty();
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

    private List<Contrib> updateWay(long wayId, List<OSMWay> newVersions, OSMWay before) throws Exception {
        var osh = new ArrayList<OSMWay>(newVersions.size() + 1);
        if (before != null) {
            osh.add(before);
        }
        osh.addAll(newVersions);

        var refIds = new HashSet<Long>();
        osh.forEach(osm -> refIds.addAll(osm.refs()));

        var nodes = store.nodes(refIds).values().stream()
                .map(node -> node.withChangeset(-1))
                .collect(Collectors.toMap(OSMNode::id, List::of));

        // update nodes with new node versions in osc
        refIds.stream()
                .filter(newNodes::containsKey)
                .map(newNodes::get)
                .forEach(entity -> {
                    var oshNode = new ArrayList<OSMNode>(entity.newVersions().size());
                    if (entity.before() != null) {
                        oshNode.add(entity.before());
                    }
                    oshNode.addAll(entity.newVersions());
                    nodes.put(oshNode.getFirst().id(), oshNode);
                });


        var changesetIds = new HashSet<Long>();
        nodes.forEach((nodeId, oshNode) -> oshNode.forEach(osm -> changesetIds.add(osm.changeset())));
        osh.forEach(osm -> changesetIds.add(osm.changeset()));
        var changesets = fetchChangesets(changesetIds, changesetDb);

        var contributions = new ContributionsWay(osh, nodes);
        var contribs = getContribs(contributions, before, changesets);

        var last = contribs.getLast();
        var osm = osh.getLast();
        var minorVersion = last.getOsmMinorVersion();
        var edits = last.getOsmEdits();
        updatedWays.put(osm.id(), osm.withMinorAndEdits(minorVersion, edits));

        return contribs;
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

    private <T extends OSMEntity> Map<Long, Entity<T>> filter(Map<Long, List<T>> newVersions, Map<Long, T> versionBefore) {
        var filtered = new HashMap<Long, Entity<T>>();
        newVersions.forEach((id, osh) -> {
            var before = versionBefore.get(id);
            if (osh.isEmpty()) {
                // minor edits
                filtered.put(id, new Entity<>(osh, before));
                return;
            }
            if (before != null) {
                osh.removeIf(version -> version.version() <= before.version());
            } else {
                osh.removeIf(not(OSMEntity::visible));
            }

            if (!osh.isEmpty()) {
                filtered.put(id, new Entity<>(osh, before));
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
        var nodeWaysBackRefs = store.backRefsNodeWay(newNodes.keySet());
        nodeWaysBackRefs.forEach((nodeId, ways) -> ways.forEach(wayId -> newVersions.computeIfAbsent(wayId, x -> List.of())));

        var versionBefore = new HashMap<>(store.ways(newVersions.keySet()));
        return filter(newVersions, versionBefore);
    }

}
