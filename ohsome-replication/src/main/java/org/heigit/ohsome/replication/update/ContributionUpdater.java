package org.heigit.ohsome.replication.update;

import com.google.common.collect.Iterators;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.contributions.contrib.*;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.changesets.Changesets;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toMap;
import static org.heigit.ohsome.contributions.util.Utils.fetchChangesets;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

public class ContributionUpdater {
    public record Entity<T extends OSMEntity>(List<T> newVersions, T before) {}

    private final Changesets changesetDb = Changesets.NOOP;
    private final SpatialJoiner countryJoiner = SpatialJoiner.NOOP;
    private final UpdateStore store;

    private Map<Long, Entity<OSMNode>> newNodes;
    private Map<Long, Entity<OSMWay>> newWays;

    public ContributionUpdater(UpdateStore store) {
        this.store = store;
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
        store.updateNodes(newNodes.values().stream()
                .map(Entity::newVersions)
                .map(List::getLast)
                .collect(toMap(OSMEntity::id, identity())));

        store.updateWays(newWays.values().stream()
                .map(Entity::newVersions)
                .filter(not(List::isEmpty))
                .map(List::getLast)
                .collect(toMap(OSMEntity::id, identity()))
        );

        updateNodeWayBackRefs();
    }

    private void updateNodeWayBackRefs() {
        var nodeWayBackRefsToExists = new HashMap<Long, Set<Long>>();
        var nodeWayBackRefsToRemove = new HashMap<Long, Set<Long>>();

        newWays.forEach((wayId, way) -> {
            var refs = new HashSet<Long>();
            way.newVersions().forEach(osm -> refs.addAll(osm.refs()));
            if (way.before() != null) {
                way.before().refs().stream()
                        .filter(not(refs::contains))
                        .forEach(refToRemove -> nodeWayBackRefsToRemove.computeIfAbsent(refToRemove, x -> new HashSet<>()).add(wayId));
            }
            refs.forEach(ref ->nodeWayBackRefsToExists.computeIfAbsent(ref, x -> new HashSet<>()).add(wayId));
        });

        var refIds = new HashSet<>(nodeWayBackRefsToExists.keySet());
        refIds.addAll(nodeWayBackRefsToRemove.keySet());
        var nodeWayBackRefs = new HashMap<>(store.getNodeWayBackRefs(refIds));
        for(var nodeId : refIds) {
            var wayBackRefs = nodeWayBackRefs.computeIfAbsent(nodeId, x -> new HashSet<>());
            wayBackRefs.addAll(nodeWayBackRefsToExists.getOrDefault(nodeId, emptySet()));
            nodeWayBackRefsToRemove.getOrDefault(nodeId, emptySet()).forEach(wayBackRefs::remove);
        }
        store.updateNodeWayBackRefs(nodeWayBackRefs);
    }

    public Flux<Contrib> updateNodes(Iterator<OSMEntity> osc) {
        newNodes = newNodes(osc);
        return Flux.fromIterable(newNodes.values())
                .flatMapSequential(entity -> fromCallable(() -> updateNode(entity.newVersions(), entity.before())).subscribeOn(parallel()))
                .flatMapIterable(identity());
    }

    public Flux<Contrib> updateWays(Iterator<OSMEntity> osc) {
        newWays = newWays(osc);
        return Flux.fromIterable(newWays.values())
                .flatMapSequential(entity -> fromCallable(() -> updateWay(entity.newVersions(), entity.before())).subscribeOn(parallel()))
                .flatMapIterable(identity());
    }

    public Flux<Contrib> updateRelations(Iterator<OSMEntity> osc) {

        return Flux.empty();
    }


    private List<Contrib> updateNode(List<OSMNode> newVersions, OSMNode before) throws Exception {
        var osh = new ArrayList<OSMNode>(newVersions.size() + 1);
        if (before != null) {
            osh.add(before);
        }
        osh.addAll(newVersions);

        var changesetIds = osh.stream().map(OSMEntity::changeset).collect(Collectors.toSet());
        var changesets = fetchChangesets(changesetIds, changesetDb);

        var contributions = new ContributionsNode(osh);
        var skipFirst = before != null && contributions.hasNext();
        return getContribs(contributions, changesets, skipFirst);
    }

    private ArrayList<Contrib> getContribs(Contributions contributions, Map<Long, ContribChangeset> changesets, boolean skipFirst) {
        var converter = new ContributionsAvroConverter(contributions, changesets::get, countryJoiner);
        if (skipFirst && converter.hasNext()) {
            converter.next();
        }
        var updates = new ArrayList<Contrib>();
        converter.forEachRemaining(contrib -> contrib.ifPresent(updates::add));
        return updates;
    }

    private List<Contrib> updateWay(List<OSMWay> newVersions, OSMWay before) throws Exception {
        var osh = new ArrayList<OSMWay>(newVersions.size() + 1);
        if (before != null) {
            osh.add(before);
        }
        osh.addAll(newVersions);

        var refIds = new HashSet<Long>();
        osh.forEach(osm -> refIds.addAll(osm.refs()));

        var nodes = store.getNodes(refIds).values().stream()
                .collect(Collectors.toMap(OSMNode::id, List::of));

        Contribution contribBefore = null;
        if (before != null) {
            var contributions = new ContributionsWay(List.of(before), nodes);
            contribBefore = contributions.next();
        }

        // update nodes with new node versions in osc
        newNodes.forEach((id, entity) -> {
            if (!refIds.contains(id)) {
                return;
            }
            var oshNode = new ArrayList<OSMNode>(entity.newVersions().size());
            if (entity.before() != null) {
                oshNode.add(entity.before());
            }
            oshNode.addAll(entity.newVersions());
            nodes.put(id, oshNode);
        });

        var changesetIds = new HashSet<Long>();
        nodes.forEach((nodeId, oshNode) -> oshNode.forEach(osm -> changesetIds.add(osm.changeset())));
        osh.forEach(osm -> changesetIds.add(osm.changeset()));
        var changesets = fetchChangesets(changesetIds, changesetDb);

        var contributions = new ContributionsWay(osh, nodes);
        var skipFirst = contributions.hasNext() && contributions.peek().equals(contribBefore);
        return getContribs(contributions,changesets, skipFirst);
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
            }
            if (!osh.isEmpty()) {
                filtered.put(id, new Entity<>(osh, before));
            }
        });
        return filtered;
    }

    public Map<Long, Entity<OSMNode>> newNodes(Iterator<OSMEntity> osc) {
        var newVersions = getByType(osc, OSMNode.class);
        return filter(newVersions, store.getNodes(newVersions.keySet()));
    }

    public Map<Long, Entity<OSMWay>> newWays(Iterator<OSMEntity> osc) {
        var newVersions = getByType(osc, OSMWay.class);
        var nodeWaysBackRefs = store.getNodeWayBackRefs(newNodes.keySet());
        nodeWaysBackRefs.forEach((nodeId, ways) -> ways.forEach(wayId -> newVersions.computeIfAbsent(wayId, x -> List.of())));
        return filter(newVersions, store.getWays(newVersions.keySet()));
    }

}
