package org.heigit.ohsome.osm.geometry;

import org.locationtech.jts.geom.Coordinate;

import java.util.*;

public class NodeSegments {
  public Map<Coordinate, Set<Segment>> map = new HashMap<>();
  private final Map<Coordinate, Set<Segment>> dupMap = new HashMap<>();

  public void add(Segment segment) {
    var nodeASegments = map.computeIfAbsent(segment.getFirstCoordinate(), k -> new HashSet<>());
    var nodeBSegments = map.computeIfAbsent(segment.getLastCoordinate(), k -> new HashSet<>());
    if (!nodeASegments.add(segment) & !nodeBSegments.add(segment)) {
      dupMap.computeIfAbsent(segment.getFirstCoordinate(), k -> new HashSet<>()).add(segment);
      dupMap.computeIfAbsent(segment.getLastCoordinate(), k -> new HashSet<>()).add(segment);
    }
  }

  public void remove(Segment segment) {
    remove(map, segment);
  }

  private void remove(Map<Coordinate, Set<Segment>> map, Segment segment) {
    remove(map, segment.getFirstCoordinate(), segment);
    remove(map, segment.getLastCoordinate(), segment);
  }

  private void remove(Map<Coordinate, Set<Segment>> map, Coordinate node, Segment segment) {
    var segments = map.get(node);
    segments.remove(segment);
    if (segments.isEmpty()) {
      map.remove(node);
    }
  }

  public boolean isValid() {
    return map.values().stream().allMatch(segments -> segments.size() % 2 == 0);
  }

  public void filterOutRedundantSegments() {
    var nodes = dupMap.keySet().toArray();
    for (int i = 0; i < dupMap.size(); i++) {
      var node = (Coordinate) nodes[i];
      if (map.get(node).size() % 2 == 1) {
        var iterator = dupMap.get(node).iterator();
        var segment = iterator.next();
        // prefer segments with mismatched parity only at one end
        while (iterator.hasNext() && map.get(segment.getOtherCoordinate(node)).size() % 2 == 1) {
          segment = iterator.next();
        }
        remove(dupMap, segment);
        remove(segment);
        // restart loop after update
        nodes = dupMap.keySet().toArray();
        i = 0;
      }
    }
  }

  public List<List<Segment>> joinItermediateSegments() {
    List<List<Segment>> rings = new ArrayList<>();
    for (Coordinate startNode : new ArrayList<>(map.keySet())) {
      if (!map.containsKey(startNode)) {
        continue;
      }
      for (Segment segment : new ArrayList<>(map.get(startNode))) {
        var currentSegment = segment;
        var endNode = segment.getLastCoordinate();
        // skip over reverse segments
        if (endNode.equals2D(startNode)) {
          continue;
        }
        map.get(startNode).remove(segment);
        map.get(endNode).remove(segment);
        while (map.containsKey(endNode)) {
          if (map.get(endNode).size() != 1) {
            map.get(startNode).add(segment);
            map.get(endNode).add(segment);
            break;
          }
          var nodeSegmentsRemoved = map.remove(endNode);
          currentSegment = nodeSegmentsRemoved.iterator().next();
          currentSegment.setFirstCoordinate(endNode);
          var removed = currentSegment.getCoordinates();
          segment.getCoordinates().addAll(removed.subList(1, removed.size()));
          endNode = segment.getLastCoordinate();
          map.get(endNode).remove(currentSegment);
          if (endNode.equals2D(startNode)) {
            if (map.get(startNode).isEmpty()) {
              map.remove(startNode);
            }
            if (segment.getCoordinates().size() > 3) {
              rings.add(List.of(segment));
            }
            break;
          }
        }
      }
    }
    return rings;
  }

  public boolean removeMismatchedSegments() {
    var mismatchedSegments = new HashSet<Segment>();
    var ids = new HashMap<Long, Segment>();
    for (var segments : map.values()) {
      if (segments.size() % 2 == 0) {
        continue;
      }
      var iterator = segments.iterator();
      ids.clear();
      while (iterator.hasNext()) {
        var segment = iterator.next();
        if (mismatchedSegments.contains(segment)) {
          continue;
        }
        var id = segment.wayId;
        if (ids.containsKey(id)) {
          ids.remove(id);
        }
        else {
          ids.put(id, segment);
        }
      }
      if (ids.size() == 1) {
        mismatchedSegments.add(ids.values().iterator().next());
      }
    }
    if (mismatchedSegments.isEmpty()) {
      return false;
    }
    mismatchedSegments.forEach(this::remove);
    return true;
  }
}
