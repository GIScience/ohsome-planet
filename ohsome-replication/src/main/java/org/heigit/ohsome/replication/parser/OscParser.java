package org.heigit.ohsome.replication.parser;


import org.heigit.ohsome.oshdb.osm.OSMCoordinates;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMMember;
import org.heigit.ohsome.osm.OSMType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.modelmbean.XMLParseException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;

import static java.lang.String.format;
import static java.time.Instant.EPOCH;
import static javax.xml.stream.XMLStreamConstants.*;

public class OscParser implements Iterator<OSMEntity>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OscParser.class);

    private static final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

    private final XMLStreamReader reader;

    private long id = -1;
    private int version = -1;
    private Instant timestamp = EPOCH;
    private long changeset = -1;
    private int uid = -1;
    private String user = "";
    private boolean visible = false;
    private final List<OSMMember> members = new ArrayList<>();
    private final Map<String, String> tags = new HashMap<>();

    private Exception exception = null;
    private OSMEntity next = null;
    private double lon;
    private double lat;

    public OscParser(InputStream input) throws XMLStreamException, XMLParseException {
        this.reader = xmlInputFactory.createXMLStreamReader(input, "UTF8");

        var eventType = reader.nextTag();
        if (eventType != START_ELEMENT) {
            throw new XMLParseException("start of element");
        }
        var localName = reader.getLocalName();
        if (!"osmChange".equals(localName)) {
            throw new XMLParseException(format("expecting tag(osmChange) but got %s", localName));
        }
        openChangeContainer();
    }


    private int lonLatConversion(double d) {
        return (int) (d * OSMCoordinates.GEOM_PRECISION_TO_LONG);
    }


    private boolean openChangeContainer() throws XMLParseException, XMLStreamException {
        int eventType = nextEvent(reader);
        if (eventType == END_ELEMENT || eventType == END_DOCUMENT) {
            return false;
        }
        if (eventType != START_ELEMENT) {
            throw new XMLParseException("start of element");
        }

        var localName = reader.getLocalName();
        if ("create".equals(localName) || "modify".equals(localName)) {
            visible = true;
        } else if ("delete".equals(localName)) {
            visible = false;
        } else {
            throw new XMLParseException("expecting tag (create/modify/delete) but got " + localName);
        }
        return true;
    }

    private void parseAttributes() throws XMLParseException {
        var attributeCount = reader.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            var attrName = reader.getAttributeLocalName(i);
            var attrValue = reader.getAttributeValue(i);
            if ("id".equals(attrName)) {
                id = Long.parseLong(attrValue);
            } else if ("version".equals(attrName)) {
                version = Integer.parseInt(attrValue);
            } else if ("timestamp".equals(attrName)) {
                timestamp = Instant.parse(attrValue);
            } else if ("uid".equals(attrName)) {
                uid = Integer.parseInt(attrValue);
            } else if ("user".equals(attrName)) {
                user = attrValue;
            } else if ("changeset".equals(attrName)) {
                changeset = Long.parseLong(attrValue);
            } else if ("lon".equals(attrName)) {
                lon = Double.parseDouble(attrValue);
            } else if ("lat".equals(attrName)) {
                lat = Double.parseDouble(attrValue);
            } else {
                throw new XMLParseException("unknown attribute: " + attrName);
            }
        }
    }

    private void parseTag() throws XMLParseException {
        String key = null;
        String value = null;
        int attributeCount = reader.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            var attrName = reader.getAttributeLocalName(i);
            var attrValue = reader.getAttributeValue(i);
            if ("k".equals(attrName)) {
                key = attrValue;
            } else if ("v".equals(attrName)) {
                value = attrValue;
            } else {
                unknownAttribute(attrName);
            }
        }

        if (key == null || value == null) {
            throw new XMLParseException(format("missing key(%s) or value(%s)", key, value));
        }
        tags.put(key, value);
    }

    private static void unknownAttribute(String attrName) throws XMLParseException {
        throw new XMLParseException(format("unknown attribute: %s", attrName));
    }

    private void parseWayMember() throws XMLParseException {
        var memberId = -1L;
        var attributeCount = reader.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            var attrName = reader.getAttributeLocalName(i);
            var attrValue = reader.getAttributeValue(i);
            if ("ref".equals(attrName)) {
                memberId = Long.parseLong(attrValue);
            } else {
                unknownAttribute(attrName);
            }
        }
        if (memberId < 0) {
            throw new XMLParseException("missing member id!");
        }
        members.add(new OSMMember(memberId));
    }

    private void parseMember() throws XMLParseException {
        String type = null;
        long ref = -1;
        String role = null;
        var attributeCount = reader.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            var attrName = reader.getAttributeLocalName(i);
            var attrValue = reader.getAttributeValue(i);
            if ("type".equals(attrName)) {
                type = attrValue;
            } else if ("ref".equals(attrName)) {
                ref = Long.parseLong(attrValue);
            } else if ("role".equals(attrName)) {
                role = attrValue;
            } else {
                unknownAttribute(attrName);
            }
        }
        if (type == null || ref < 0 || role == null) {
            throw new XMLParseException(format("missing member attribute (%s,%d,%s)", type, ref, role));
        }
        members.add(new OSMMember(OSMType.valueOf(type.toUpperCase()), ref, role));
    }

    private void clearVariables() {
        id = changeset = uid = version = -1;
        timestamp = EPOCH;
        user = "";
        lon = lat = -999.9;
        members.clear();
        tags.clear();
    }


    private void parseEntity() throws XMLStreamException, XMLParseException {
        clearVariables();
        parseAttributes();
        int eventType;

        while ((eventType = reader.nextTag()) == START_ELEMENT) {
            String localName = reader.getLocalName();
            if ("tag".equals(localName)) {
                parseTag();
            } else if ("nd".equals(localName)) {
                parseWayMember();
            } else if ("member".equals(localName)) {
                parseMember();
            } else {
                throw new XMLParseException("unexpected tag, expect tag/nd/member but got " + localName);
            }
            eventType = reader.nextTag();
            if (eventType != END_ELEMENT) {
                throw new XMLParseException("unclosed " + localName);
            }
        }
        if (eventType != END_ELEMENT) {
            throw new XMLParseException(format("expect tag end but got %s", eventType));
        }
    }

    private OSMNode nextNode() throws XMLParseException, XMLStreamException {
        parseEntity();
        if (visible && !validCoordinate(lon, lat)) {
            throw new XMLParseException(format("invalid coordinates! lon:%f lat:%f", lon, lat));
        }

        LOG.debug("node/{} {} {} {} {} {} {} {} {} {}", id, version, visible, timestamp, changeset,
                user, uid, tags, lon, lat);
        return new OSMNode(id, version(version, visible), timestamp, changeset, uid, user, visible, tags, lon, lat);
    }

    private Integer version(Integer version, boolean visible) {
        return visible ? version : -version;
    }

    private boolean validCoordinate(double lon, double lat) {
        return Math.abs(lon) <= 180.0 && Math.abs(lat) <= 90.0;
    }

    private OSMWay nextWay() throws XMLStreamException, XMLParseException {
        parseEntity();
        LOG.debug("way/{} {} {} {} {} {} {} {} {}", id, version, visible, timestamp, changeset, user,
                uid, tags, members.size());
        return new OSMWay(id, version(version, visible), timestamp, changeset, uid, user, visible, tags,
                members.stream().map(OSMMember::id).toList());
    }

    private OSMRelation nextRelation() throws XMLStreamException, XMLParseException {
        parseEntity();
        LOG.debug("relation/{} {} {} {} {} {} {} {} mems:{}", id, version, visible, timestamp,
                changeset, user, uid, tags, members.size());
        return new OSMRelation(id, version(version, visible), timestamp, changeset, uid, user, visible, tags,
                members);
    }

    private OSMEntity computeNext() {
        try {
            var eventType = nextEvent(reader);
            if (eventType == END_DOCUMENT) {
                return null;
            }

            if (eventType == END_ELEMENT) {
                if (!openChangeContainer()) {
                    return null;
                }
                eventType = reader.nextTag();
            }
            if (eventType != START_ELEMENT) {
                throw new XMLParseException("expecting start of (node/way/relation)");
            }
            String localName = reader.getLocalName();
            if ("node".equals(localName)) {
                return nextNode();
            } else if ("way".equals(localName)) {
                return nextWay();
            } else if ("relation".equals(localName)) {
                return nextRelation();
            }
            throw new XMLParseException(format("expecting (node/way/relation) but got %s", localName));
        } catch (Exception e) {
            this.exception = e;
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || (next = computeNext()) != null;
    }

    @Override
    public OSMEntity next() {
        if (!hasNext()) {
            throw new NoSuchElementException((exception == null ? null : exception.toString()));
        }
        var r = next;
        next = null;
        return r;
    }

    private int nextEvent(XMLStreamReader reader) throws XMLStreamException {
        while (true) {
            var event = readNextEvent(reader);
            if (!event.skip) {
                return event.event;
            }
        }
    }

    private Event readNextEvent(XMLStreamReader reader) throws XMLStreamException {
        var event = reader.next();
        return switch (event) {
            case SPACE, COMMENT, PROCESSING_INSTRUCTION, CDATA, CHARACTERS -> new Event(event, true);
            case START_ELEMENT, END_ELEMENT, END_DOCUMENT -> new Event(event, false);
            default -> throw new XMLStreamException(format(
                    "Received event %d, instead of START_ELEMENT or END_ELEMENT or END_DOCUMENT.",
                    event));
        };
    }

    private record Event(int event, boolean skip) {
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}

