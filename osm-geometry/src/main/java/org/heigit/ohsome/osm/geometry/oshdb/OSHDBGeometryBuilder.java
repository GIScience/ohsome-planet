package org.heigit.ohsome.osm.geometry.oshdb;

import static org.heigit.ohsome.oshdb.osm.OSMCoordinates.toOSM;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTags;
import org.heigit.ohsome.oshdb.OSHDBTemporal;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMCoordinates;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilderInternal;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

public class OSHDBGeometryBuilder {
  private static final GeometryFactory geometryFactory = new GeometryFactory();
  private static final OSHDBGeometryBuilderInternal builder = new OSHDBGeometryBuilderInternal(tagInterpreter(), geometryFactory);

  public static Geometry buildMultiPolygon(List<List<Coordinate>> outer, List<List<Coordinate>> inner) {
    var geometry =  builder.getMultiPolygonGeometry(
        new ArrayList<>(outer.stream().map(OSHDBGeometryBuilder::line).toList()),
        new ArrayList<>(inner.stream().map(OSHDBGeometryBuilder::line).toList())
    );
    if (geometry instanceof Polygon polygon) {
      return geometryFactory.createMultiPolygon(new Polygon[]{polygon});
    }
    return geometry;
  }

  private static LinkedList<OSMNode> line(List<Coordinate> coordinates) {
    var line = new LinkedList<OSMNode>();
    coordinates.forEach(coordinate -> line.add(node(coordinate)));
    return line;
  }

  private static OSMNode node(Coordinate coordinate) {
    var node = OSM.node(id(coordinate), 1, 0, 0, 0, List.of(),
        toOSM(coordinate.x), toOSM(coordinate.y));
    return new Node(node);
  }

  private static long id(Coordinate coordinate) {
    var id = (long) toOSM(coordinate.x);
    return  (id << 32) | toOSM(coordinate.y);
  }

  private static int toOSM(double value) {
    return (int) Math.round(value * OSMCoordinates.GEOM_PRECISION_TO_LONG);
  }

  private static class Node implements OSMNode{
    private final OSMNode node;

    private Node(OSMNode node) {
      this.node = node;
    }

    public OSMType getType() {
      return node.getType();
    }

    public boolean isVisible() {
      return node.isVisible();
    }

    public double getLongitude() {
      return node.getLon() / OSMCoordinates.GEOM_PRECISION_TO_LONG;
    }

    public int getUserId() {
      return node.getUserId();
    }

    public double getLatitude() {
      return node.getLat() / OSMCoordinates.GEOM_PRECISION_TO_LONG;
    }

    public long getChangesetId() {
      return node.getChangesetId();
    }

    public int getVersion() {
      return node.getVersion();
    }

    public int getLon() {
      return node.getLon();
    }

    public long getEpochSecond() {
      return node.getEpochSecond();
    }

    public int getLat() {
      return node.getLat();
    }

    public long getId() {
      return node.getId();
    }

    public boolean isBefore(OSHDBTemporal other) {
      return node.isBefore(other);
    }

    public OSHDBTags getTags() {
      return node.getTags();
    }

    public boolean isAfter(OSHDBTemporal other) {
      return node.isAfter(other);
    }



  }

  private static TagInterpreter tagInterpreter() {
    return new TagInterpreter() {
      @Override
      public boolean isArea(OSMEntity osmEntity) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isLine(OSMEntity osmEntity) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasInterestingTagKey(OSMEntity osmEntity) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isMultipolygonOuterMember(OSMMember osmMember) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isMultipolygonInnerMember(OSMMember osmMember) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isOldStyleMultipolygon(OSMRelation osmRelation) {
        throw new UnsupportedOperationException();
      }
    };
  }

}
