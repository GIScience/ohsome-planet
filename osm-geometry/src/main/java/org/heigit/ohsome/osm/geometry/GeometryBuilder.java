package org.heigit.ohsome.osm.geometry;

import java.util.*;

import org.locationtech.jts.geom.*;

import static java.util.Comparator.comparing;

public class GeometryBuilder {
  private static final GeometryFactory geometryFactory = new GeometryFactory();

  public enum Mode {
    DEFAULT,
    LEGACY
  }

  public static Geometry buildMultiPolygon(List<List<Coordinate>> outer, List<List<Coordinate>> inner) {
    return buildMultiPolygon(outer, inner, Mode.DEFAULT);
  }

  public static Geometry buildMultiPolygonLegacy(List<List<Coordinate>> outer, List<List<Coordinate>> inner) {
    return buildMultiPolygon(outer, inner, Mode.LEGACY);
  }

  public static Geometry buildMultiPolygon(List<List<Coordinate>> outer, List<List<Coordinate>> inner, Mode mode) {
    var merged = new ArrayList<List<Coordinate>>(outer.size() + inner.size());
    merged.addAll(outer);
    merged.addAll(inner);
    var polygonList = new ArrayList<>(buildPolygons(merged, mode));

    var matchedPolygons = new HashMap<Polygon, ArrayList<Polygon>>();
    var isInner = new boolean[polygonList.size()];
    polygonList.sort(comparing(Polygon::getArea).reversed());
    for (int i = 0; i < polygonList.size(); i++) {
      var currPoly = polygonList.get(i);
      for (int j = i - 1; j >= 0; j--) {
        var prevPoly = polygonList.get(j);
        if (prevPoly.contains(currPoly)) {
          if (!isInner[j]) {
            matchedPolygons.get(prevPoly).add(currPoly);
            isInner[i] = true;
          }
          break;
        }
      }
      if (!isInner[i]) {
        matchedPolygons.put(currPoly, new ArrayList<>());
      }
    }

    var matchedPolygonsOuter = matchedPolygons.keySet().stream().toList();
    var outerPolygons = new ArrayList<>(matchedPolygonsOuter);
    if (handleIntersectingAndTouchingPolygons(outerPolygons, false)) {
      for (int i = 0; i < matchedPolygonsOuter.size(); i++) {
        var originalPoly = matchedPolygonsOuter.get(i);
        var modifiedPoly = outerPolygons.get(i);
        if (originalPoly == modifiedPoly) {
          continue;
        }
        var innerPolygons = matchedPolygons.remove(originalPoly);
        for (var innerPoly : innerPolygons) {
          if (!modifiedPoly.contains(innerPoly)) {
            throw new GeometryBuilderException("Overlapping outer rings with an inner ring at intersection");
          }
        }
        matchedPolygons.put(modifiedPoly, innerPolygons);
      }
    }

    var polygons = new ArrayList<Polygon>();
    outerpoly:
    while (!matchedPolygons.isEmpty()) {
      var outerPoly = matchedPolygons.keySet().iterator().next();
      var outerRing = outerPoly.getExteriorRing();
      var innerRings = new ArrayList<LinearRing>();
      var innerPolygons = matchedPolygons.remove(outerPoly);
      if (handleIntersectingAndTouchingPolygons(innerPolygons, true)) {
        var multipolygon = outerPoly.difference(geometryFactory.createMultiPolygon(innerPolygons.toArray(Polygon[]::new)));
        for (int i = 0; i < multipolygon.getNumGeometries(); i++) {
          var polygon = (Polygon) multipolygon.getGeometryN(i);
          polygons.add(polygon);
        }
        continue;
      }
      for (var innerPoly : innerPolygons) {
        var innerRing = innerPoly.getExteriorRing();
        if (innerRing.getEnvelope().intersects(outerRing) && innerRing.intersects(outerRing)) {
          var ringIntersection = innerRing.intersection(outerRing);
          if (!(ringIntersection instanceof Point)) {
            var difference = outerPoly.difference(innerPoly);
            innerPolygons.remove(innerPoly);
            for (int i = 1; i < difference.getNumGeometries(); i++) {
              var outerPolygon = (Polygon) difference.getGeometryN(i);
              var matchedInnerPolygons = new ArrayList<Polygon>();
              for (int j = 0; j < innerPolygons.size(); j++) {
                if (outerPolygon.contains(innerPolygons.get(j))) {
                  matchedInnerPolygons.add(innerPolygons.remove(j--));
                }
              }
              matchedPolygons.put(outerPolygon, matchedInnerPolygons);
            }
            matchedPolygons.put((Polygon) difference.getGeometryN(0), innerPolygons);
            continue outerpoly;
          }
        }
        innerRings.add(innerRing);
      }
      polygons.add(geometryFactory.createPolygon(outerRing, innerRings.toArray(LinearRing[]::new)));
    }
    return geometryFactory.createMultiPolygon(polygons.toArray(Polygon[]::new));
  }

  private static boolean handleIntersectingAndTouchingPolygons(List<Polygon> polygons, boolean mergeTouching) {
    boolean formsEnclosedAreas = false;
    for (int i = 0; i < polygons.size(); i++) {
      for (int j = i + 1; j < polygons.size(); j++) {
        var polyA = polygons.get(i);
        var ringA = polyA.getExteriorRing();
        var polyB = polygons.get(j);
        var ringB = polyB.getExteriorRing();
        if (ringA.disjoint(ringB)) {
          continue;
        }
        var ringIntersection = ringA.intersection(ringB);
        var intersectionCoordinates = Arrays.asList(ringIntersection.getCoordinates());
        if (!new HashSet<>(Arrays.asList(polyA.getCoordinates())).containsAll(intersectionCoordinates) ||
                !new HashSet<>(Arrays.asList(polyB.getCoordinates())).containsAll(intersectionCoordinates)) {
          if (ringIntersection instanceof Point) {
            throw new GeometryBuilderException("Touching polygons without common node");
          }
          if (ringIntersection instanceof MultiPoint) {
            throw new GeometryBuilderException("Overlapping polygons");
          }
        }
        if (ringIntersection instanceof Point) {
          formsEnclosedAreas = true;
        } else if (ringIntersection instanceof MultiPoint) {
          formsEnclosedAreas = true;
          if (ringIntersection.equals(polyA.intersection(polyB))) {
            continue;
          }
          if (polyA.difference(polyB) instanceof Polygon polyAdiff && polyB.difference(polyA) instanceof Polygon polyBdiff) {
            polygons.set(i, polyAdiff);
            polygons.set(j, polyBdiff);
          } else {
            throw new GeometryBuilderException("Unexpected result of multipolygon subtraction");
          }
        } else if (ringIntersection instanceof LineString || ringIntersection instanceof MultiLineString) {
          if (mergeTouching) {
            polygons.set(i, (Polygon) polyA.union(polyB));
            polygons.remove(j--);
          } else {
            throw new GeometryBuilderException("Overlapping outer rings");
          }
        }
      }
    }
    return formsEnclosedAreas;
  }

  private static List<List<Segment>> buildRings(List<List<Coordinate>> ways, Mode mode) {
    var nodeSegments = new NodeSegments();

    // split ways into segments consisting of pairs of nodes and create a mapping of nodes to segments
    for (Segment segment : splitWays(ways)) {
      if (segment.getFirstCoordinate().equals2D(segment.getLastCoordinate())) {
        continue;
      }
      nodeSegments.add(segment);
    }

    nodeSegments.filterOutRedundantSegments();

    var rings = nodeSegments.joinItermediateSegments();

    if (!nodeSegments.isValid()) {
      if (mode == Mode.DEFAULT) {
        throw new GeometryBuilderException("Mismatched segments parity");
      }
      if (nodeSegments.removeMismatchedSegments()) {
        rings.addAll(nodeSegments.joinItermediateSegments());
      }
    }

    while (!nodeSegments.map.isEmpty()) {
      var segment = nodeSegments.map.values().iterator().next().iterator().next();
      var startNode = segment.getFirstCoordinate();
      var nextNode = segment.getLastCoordinate();
      var nextNodeSegments = nodeSegments.map.get(nextNode);
      nodeSegments.remove(segment);
      if (nextNodeSegments.isEmpty()) {
        continue;
      }
      var currentRing = new ArrayList<Segment>();
      rings.add(currentRing);
      currentRing.add(segment);
      var segmentsIterator = nextNodeSegments.iterator();
      var wayId = segment.wayId;
      boolean found = false;
      do {
        var nextSegment = segmentsIterator.next();
        if (nextSegment.getOtherCoordinate(nextNode).equals2D(startNode)) {
          found = true;
          segment = nextSegment;
          if (wayId == segment.wayId) {
            break;
          }
        }
      } while (segmentsIterator.hasNext());
      if (!found) {
        throw new GeometryBuilderException("Failed to find next segment");
      }

      segment.setFirstCoordinate(nextNode);
      currentRing.add(segment);
      nodeSegments.remove(segment);
    }

    return rings;
  }

  private static List<Segment> splitWays(List<List<Coordinate>> ways) {
    var segments = new ArrayList<Segment>();
    var uniqueWays = new HashSet<Set<Coordinate>>();

    long wayId = 0;
    for (List<Coordinate> way : ways) {
      if (way.size() < 2) {
        continue;
      }
      if (!uniqueWays.add(new HashSet<>(way))) {
        continue;
      }
      var prevCoord = way.getFirst();
      for (int i = 1; i < way.size(); i++) {
        var nextCoord = way.get(i);
        segments.add(new Segment(List.of(prevCoord, nextCoord), wayId));
        prevCoord = nextCoord;
      }
      wayId++;
    }

    return segments;
  }

  private static List<Polygon> buildPolygons(List<List<Coordinate>> ways, Mode mode) {
    var polygonList = buildRings(ways, mode).stream().map(GeometryBuilder::mergeSegments).map(geometryFactory::createPolygon).toList();
    if (polygonList.stream().allMatch(Geometry::isValid)) {
      return polygonList;
    } else {
      throw new GeometryBuilderException("Invalid polygon");
    }
  }

  private static Coordinate[] mergeSegments(List<Segment> segments) {
    List<Coordinate> coordinates = new ArrayList<>();
    for (Segment segment : segments) {
      List<Coordinate> coords = segment.toCoordinates();
      coordinates.addAll(coords.subList(0, coords.size() - 1));
    }

    //remove overlapping segments
    var a = coordinates.getLast();
    var b = coordinates.getFirst();
    var ab = geometryFactory.createLineString(new Coordinate[]{a, b});
    for (int i = 1; i < coordinates.size(); i++) {
      var c = coordinates.get(i);
      var bc = geometryFactory.createLineString(new Coordinate[]{b, c});
      if (ab.covers(bc) || bc.covers(ab)) {
        coordinates.remove(b);
        ab = geometryFactory.createLineString(new Coordinate[]{a, c});
        i--;
      } else {
        a = b;
        ab = bc;
      }
      b = c;
    }

    //close ring
    coordinates.add(coordinates.getFirst());

    return coordinates.toArray(Coordinate[]::new);
  }

}
