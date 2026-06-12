package org.heigit.ohsome.osm.geometry;

import org.heigit.ohsome.osm.geometry.OSMParser.Member;
import org.heigit.ohsome.osm.geometry.assembler.GeometryAssembler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;

class GeometryBuilderTest {

    private static final GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(1e7));
    private static final WKTReader WKT_READER = new WKTReader(FACTORY);

    static List<OSMTest> OSM_TEST_CASES;

    static {
        var path = Path.of("src/test/resources/osm-testdata/grid/data/7").toAbsolutePath();
        try (var lines = Files.list(path).filter(Files::isDirectory)) {
            OSM_TEST_CASES = lines.flatMap(OSMTest::loadSilent).sorted(Comparator.comparing(OSMTest::testId)).toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Set<Integer> DISABLED_OSM_TESTS = Set.of(-1
            , 749 // invalid

            , 760, 761, 762 // slightly different result!

            , 791 // Multipolygon relation containing the two ways using the same nodes in the same order.
            , 792 // Multipolygon relation containing two ways using the same nodes in different order.
            , 793 // Multipolygon relation containing the two ways using nearly the same nodes.
            , 784 // modified by 7840
            , 785 // modified by 7850


            // DISABLE for now!
            , 777, 778, 779 // Multipolygon with two outer rings and two inner rings touching in two nodes.
    );

    static Stream<OSMTest> osmAllTest() {
        return OSM_TEST_CASES.stream().filter(test -> !DISABLED_OSM_TESTS.contains(test.testId()));
    }

    static Stream<OSMTest> osmAllValidTest() {
        return osmAllTest().filter(OSMTest::isValidWithFix);
    }

    static OSMTest osmTest(int testId) {
        return OSM_TEST_CASES.stream().filter(t -> t.testId() == testId).findFirst().orElseThrow();
    }

    @ParameterizedTest
    @MethodSource("osmAllValidTest")
    void osmTestCases(OSMTest test) throws ParseException {
        var assembler = new GeometryAssembler();
        var inner = Optional.ofNullable(test.osm().relations()).stream()
                .map(List::getFirst)
                .map(OSMParser.Relation::members).<Member>mapMulti(Iterable::forEach)
                .filter(m -> m.type().equals("way") && m.role().equals("inner")).map(Member::ref).collect(toSet());
        var geometry = assembler.assemble(test.ways(), inner);
        var expected = WKT_READER.read(test.resultWithFix().wkt()).norm();
        if (test.isValid() || test.hasFix() && geometry != null) {
            assertNotNull(geometry, "Null geometry for " + test.testId() + ": " + test.description());
            assertEquals(expected, geometry.norm(), "Test " + test.testId() + ": " + test.description());
            assertTrue(geometry.isValid(), "Invalid " + test.testId() + ": " + test.description());
        } else {
            assertNull(geometry, "Expected null geometry for " + test.testId() + ": " + test.description());
        }
    }

    @Disabled
    @Test
    void osmDebugTestCases() throws ParseException {
        var test = osmTest(764);
        var expected = WKT_READER.read(test.defaultResult().wkt()).norm();
        System.out.println(expected);
        var assembler = new GeometryAssembler();
        var geometry = assembler.assemble(test.ways(), Set.of());
        System.out.println(geometry);
        assertNotNull(geometry, "Null geometry for " + test.testId() + ": " + test.description());
        assertEquals(expected, geometry.norm(), "Test " + test.testId() + ": " + test.description());
        assertTrue(geometry.isValid(), "Invalid " + test.testId() + ": " + test.description());
    }

    @Disabled
    @Test
    void testInvalidGeometry() throws IOException {
        var debugId = 23236;
        var ways = new HashMap<Long, LineString>();
        try (var lines = Files.lines(Path.of("assemble_debug_%d_ways.wkt".formatted(debugId)))) {
            lines.forEach(line -> {
                var parts = line.split(";");
                var id = Long.parseLong(parts[0]);
                Geometry wkt;
                try {
                    wkt = WKT_READER.read(parts[1]);
                    ways.put(id, (LineString) wkt);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        var assembler = new GeometryAssembler();
        var geometry = assembler.assemble(ways, Set.of());
        assertNotNull(geometry, "Expected non-null geometry for debug test " + debugId);
        System.out.println(geometry);
        assertTrue(geometry.isValid(), "Expected valid geometry for debug test " + debugId);
    }


}