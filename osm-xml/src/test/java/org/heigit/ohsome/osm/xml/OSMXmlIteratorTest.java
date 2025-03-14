package org.heigit.ohsome.osm.xml;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import javax.management.modelmbean.XMLParseException;
import javax.xml.stream.XMLStreamException;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;
import org.junit.jupiter.api.Test;

class OSMXmlIteratorTest {

  @Test
  void testXml() throws XMLStreamException, XMLParseException {
    var iterator = new OSMXmlIterator(new ByteArrayInputStream(
        """
            <?xml version='1.0' encoding='UTF-8'?>
            <osm version="0.6" generator="testdata" upload="false">
                <node id="707000" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.71" lat="1.04"/>
                <node id="707001" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.72" lat="1.06"/>
                <node id="707002" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.75" lat="1.05"/>
                <node id="707003" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.73" lat="1.02"/>
                <node id="707004" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.74" lat="1.03"/>
                <node id="707005" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.77" lat="1.03"/>
                <node id="707006" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.77" lat="1.01"/>
                <node id="707007" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1" lon="7.74" lat="1.01"/>
                <way id="707800" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1">
                    <nd ref="707000"/>
                    <nd ref="707001"/>
                    <nd ref="707002"/>
                    <nd ref="707003"/>
                    <nd ref="707000"/>
                    <tag k="test:section" v="mp-geom"/>
                    <tag k="test:id" v="707"/>
                </way>
                <way id="707801" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1">
                    <nd ref="707004"/>
                    <nd ref="707005"/>
                    <nd ref="707006"/>
                    <tag k="test:section" v="mp-geom"/>
                    <tag k="test:id" v="707"/>
                </way>
                <way id="707802" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1">
                    <nd ref="707006"/>
                    <nd ref="707007"/>
                    <nd ref="707004"/>
                    <tag k="test:section" v="mp-geom"/>
                    <tag k="test:id" v="707"/>
                </way>
                <relation id="707900" version="1" timestamp="2014-01-01T00:00:00Z" uid="1" user="test" changeset="1">
                    <member type="way" ref="707800" role="outer"/>
                    <member type="way" ref="707801" role="outer"/>
                    <member type="way" ref="707802" role="outer"/>
                    <tag k="type" v="multipolygon"/>
                    <tag k="test:section" v="mp-geom"/>
                    <tag k="test:id" v="707"/>
                    <tag k="landuse" v="forest"/>
                </relation>
            </osm>""".getBytes()));
    var entities = new ArrayList<OSMEntity>();
    iterator.forEachRemaining(entities::add);

    assertEquals(12, entities.size());
    assertTrue(entities.subList(0, 8).stream().allMatch(osm -> osm.type() == OSMType.NODE));
    assertTrue(entities.subList(8, 11).stream().allMatch(osm -> osm.type() == OSMType.WAY));
    assertTrue(entities.subList(11, 12).stream().allMatch(osm -> osm.type() == OSMType.RELATION));

    assertEquals("707",entities.get(10).tags().get("test:id"));


  }

}