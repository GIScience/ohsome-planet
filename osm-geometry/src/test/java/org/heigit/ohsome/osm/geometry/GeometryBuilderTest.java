package org.heigit.ohsome.osm.geometry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.geometry.oshdb.OSHDBGeometryBuilder;
import org.heigit.ohsome.osm.xml.OSMXmlIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.heigit.ohsome.osm.OSMType.WAY;
import static org.junit.jupiter.api.Assertions.*;

class GeometryBuilderTest {
  private static Stream<String> testCases(String subdir) {
    File file = new File(GeometryBuilderTest.class.getResource("/" + subdir).getPath());
    String[] directories = file.list(new FilenameFilter() {
      @Override
      public boolean accept(File current, String name) {
        return new File(current, name).isDirectory();
      }
    });

    return Arrays.stream(directories).sorted().map(name -> subdir + "/" + name);
  }

  private static Stream<String> testCasesOsm() {
    return testCases("osm-testdata/grid/data/7");
  }

  private static Stream<String> testCasesMod() {
    return testCases("mod");
  }

  @ParameterizedTest
  @MethodSource("testCasesOsm")
  void test7xxNew(String testId) throws Exception {
    test7xx(testId, GeometryBuilder::buildMultiPolygon);
  }

  @ParameterizedTest
  @MethodSource("testCasesMod")
  void test7xxMod(String testId) throws Exception {
    test7xx(testId, GeometryBuilder::buildMultiPolygonLegacy);
  }

  @Disabled
  @ParameterizedTest
  @MethodSource("testCasesOsm")
  void test7xxOSHDB(String testId) throws Exception {
    test7xx(testId, OSHDBGeometryBuilder::buildMultiPolygon);
  }

  void test7xx(String testId, Builder builder) throws Exception {
    try (var data = this.getClass().getResourceAsStream("/" + testId + "/data.osm");
         var test = this.getClass().getResourceAsStream("/" + testId + "/test.json");
         var xml = new OSMXmlIterator(data)) {
      var nodes = new HashMap<Long, OSMNode>();
      var ways = new HashMap<Long, List<OSMNode>>();
      OSMRelation relation = null;
      while(xml.hasNext()) {
        var osm = xml.next();
        switch (osm) {
          case OSMNode node -> nodes.put(node.id(), node);
          case OSMWay way -> ways.put(way.id(), way.refs().stream().map(nodes::get).toList());
          case OSMRelation rel -> {
            assertNull(relation);
            relation = rel;
          }
        }
      }

      List<List<OSMNode>> outerWays;
      List<List<OSMNode>> innerWays;
      if (relation == null) {
        assertEquals(1, ways.size());
        outerWays = ways.values().stream().toList();
        innerWays = List.of();
      } else {
        outerWays = relation.members().stream().filter(mem -> mem.type().equals(WAY)).filter(mem -> "outer".equals(mem.role()) || mem.role().isBlank()).map(mem -> ways.get(mem.id())).toList();
        innerWays = relation.members().stream().filter(mem -> mem.type().equals(WAY)).filter(mem -> "inner".equals(mem.role())).map(mem -> ways.get(mem.id())).toList();
      }

      var mapping = new HashMap<OSMNode, Coordinate>();
      outerWays.forEach(way -> way.forEach(node -> mapping.computeIfAbsent(node, n -> new Coordinate(n.lon(), n.lat()))));
      innerWays.forEach(way -> way.forEach(node -> mapping.computeIfAbsent(node, n -> new Coordinate(n.lon(), n.lat()))));
      var outer = outerWays.stream().map(way -> way.stream().map(mapping::get).toList()).toList();
      var inner = innerWays.stream().map(way -> way.stream().map(mapping::get).toList()).toList();

      var json = new ObjectMapper().readTree(test);
      var area = json.findPath("areas");
      var node = area.get("default");
      if (area.has("fix")) {
        node = area.get("fix");
      } else if (area.has("location")) {
        node = area.get("location");
      }
      var reference = node.get(0).get("wkt").textValue();

      var result = "INVALID";
      System.out.println(testId + ": " + json.findPath("description").textValue());
      try {
        var geometry = builder.buildMultiPolygon(outer, inner);
        result = (geometry.isValid() ? "" : "INVALID ") + (geometry.isEmpty() ? result : geometry.toText());

        if (!"INVALID".equals(reference) && geometry.isValid()) {
          var expected = new org.locationtech.jts.io.WKTReader().read(reference);
          if (expected.equalsTopo(geometry)) {
            return;
          }
        }
      } catch (GeometryBuilderException ignored) {
        // ignore this exception
      }

      assertEquals(reference, result);
    }
  }
}