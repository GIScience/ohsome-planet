package org.heigit.ohsome.osm.xml;

import static java.util.Optional.ofNullable;
import static javax.xml.stream.XMLStreamConstants.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.modelmbean.XMLParseException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMMember;
import org.heigit.ohsome.osm.OSMType;

public class OSMXmlEntityReader {

  private long id = -1;
  private int version = -1;
  private boolean visible;
  private long timestamp = -1;
  private long changeset = -1;
  private int uid = -1;
  private String user = "";
  private double lon;
  private double lat;

  private final Map<String, String> tags = new HashMap<>();
  private final List<Long> refs = new ArrayList<>();
  private final List<OSMMember> members = new ArrayList<>();

  public OSMEntity entity(XMLStreamReader reader, boolean visible) throws XMLStreamException, XMLParseException {
    var eventType = nextEvent(reader);
    if (eventType == END_ELEMENT || eventType == END_DOCUMENT) {
      return null;
    }

    if (eventType != START_ELEMENT) {
      throw new XMLParseException("start of element");
    }

    var localName = reader.getLocalName();
    parseEntity(reader, visible);
    return switch (localName) {
      case "node" -> node();
      case "way" -> way();
      case "relation" -> relation();
      default -> throw new XMLParseException(
          "expecting (node/way/relation) but got %s".formatted(localName));
    };
  }

  private OSMEntity node() throws XMLParseException {
    if (Double.isNaN(lon) || Double.isNaN(lat)) {
      throw new XMLParseException("missing lon/lat %s/%s".formatted(lon, lat));
    }
    return new OSMNode(id, version, Instant.ofEpochSecond(timestamp), changeset, uid, user, visible, Map.copyOf(tags), lon, lat);
  }

  private OSMEntity way() {
    return new OSMWay(id, version, Instant.ofEpochSecond(timestamp), changeset, uid, user, visible, Map.copyOf(tags), List.copyOf(refs));
  }

  private OSMEntity relation() {
    return new OSMRelation(id, version, Instant.ofEpochSecond(timestamp), changeset, uid, user, visible, Map.copyOf(tags), List.copyOf(members));
  }

  private void parseEntity(XMLStreamReader reader, boolean visible) throws XMLStreamException, XMLParseException {
    id = timestamp = changeset = uid = version = -1;
    this.visible = visible;
    user = "";
    lon = lat = Double.NaN;
    tags.clear();
    refs.clear();
    members.clear();

    parseAttributes(reader);
    int eventType;
    while ((eventType = reader.nextTag()) == START_ELEMENT) {
      var localName = reader.getLocalName();
      switch (localName) {
        case "tag" -> parseTag(reader);
        case "nd" -> parseRef(reader);
        case "member" -> parseMember(reader);
        default -> throw new XMLParseException("unexpected tag, expect tag/nd/member but got " + localName);
      }

      eventType = reader.nextTag();
      if (eventType != END_ELEMENT) {
        throw new XMLParseException("unclosed " + localName);
      }
    }
    if (eventType != END_ELEMENT) {
      throw new XMLParseException("expect tag end but got %s".formatted(eventType));
    }
  }

  private void parseAttributes(XMLStreamReader reader) throws XMLParseException {
    var attributeCount = reader.getAttributeCount();
    for (int i = 0; i < attributeCount; i++) {
      var attrName = reader.getAttributeLocalName(i);
      var attrValue = reader.getAttributeValue(i);
      switch (attrName) {
        case "id" -> this.id = Long.parseLong(attrValue);
        case "version" -> this.version = Integer.parseInt(attrValue);
        case "visible" -> this.visible = Boolean.parseBoolean(attrValue);
        case "timestamp" -> this.timestamp = Instant.parse(attrValue).getEpochSecond();
        case "uid" -> this.uid = Integer.parseInt(attrValue);
        case "user" -> this.user = attrValue;
        case "changeset" -> this.changeset = Long.parseLong(attrValue);
        case "lon" -> this.lon = Double.parseDouble(attrValue);
        case "lat" -> this.lat = Double.parseDouble(attrValue);
        default -> throw new XMLParseException("unknown attribute: " + attrName);
      }
    }
  }

  private void parseTag(XMLStreamReader reader) throws XMLParseException {
    var key = reader.getAttributeValue(null, "k");
    var value = reader.getAttributeValue(null, "v");

    if (key == null || value == null) {
      throw new XMLParseException("tag! missing key(%s) or value(%s)".formatted(key, value));
    }
    tags.put(key, value);
  }

  private void parseRef(XMLStreamReader reader) {
    var attrValue = reader.getAttributeValue(null, "ref");
    refs.add(Long.parseLong(attrValue));
  }

  private void parseMember(XMLStreamReader reader) throws XMLParseException {
    var type = ofNullable(reader.getAttributeValue(null, "type"))
        .map(OSMType::parseType)
        .orElseThrow(() -> new XMLParseException("member missing type!"));
    var ref = ofNullable(reader.getAttributeValue(null, "ref"))
        .map(Long::parseLong)
        .orElseThrow(() -> new XMLParseException("member missing ref!"));
    var role = ofNullable(reader.getAttributeValue(null, "role"))
        .orElseThrow(() -> new XMLParseException("member missing role!"));
    members.add(new OSMMember(type, ref, role));
  }


  private int nextEvent(XMLStreamReader reader) throws XMLStreamException {
    while (true) {
      var event = readNextEvent(reader);
      if (!event.skip) {
        return event.event;
      }
    }
  }

  private record Event(int event, boolean skip) {

  }

  private Event readNextEvent(XMLStreamReader reader) throws XMLStreamException {
    var event = reader.next();
    return switch (event) {
      case SPACE, COMMENT, PROCESSING_INSTRUCTION, CDATA, CHARACTERS -> new Event(event, true);
      case START_ELEMENT, END_ELEMENT, END_DOCUMENT -> new Event(event, false);
      default -> throw new XMLStreamException(
          "Received event %d, instead of START_ELEMENT or END_ELEMENT or END_DOCUMENT.".formatted(
              event));
    };
  }


}
