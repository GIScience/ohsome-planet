package org.heigit.ohsome.osm.changesets;

import javax.management.modelmbean.XMLParseException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static javax.xml.stream.XMLStreamConstants.*;

public class ChangesetXmlReader<T> implements Iterator<T> {
    public interface Factory<T> {
        T apply(long id, Instant created, Instant closed, String user, int userId, double minLon, double minLat, double maxLon, double maxLat, int numChanges, int commentsCount, Map<String, String> tags);
    }

    private final Factory<T> factory;
    private final XMLStreamReader reader;

    private T next;

    public ChangesetXmlReader(Factory<T> factory, InputStream input) throws XMLStreamException, XMLParseException {
        this.factory = factory;
        this.reader = XMLInputFactory.newInstance().createXMLStreamReader(input);
        assertStartElement("osm");
    }

    private T computeNext() throws XMLParseException, XMLStreamException {
        var eventType = nextEvent();
        if (eventType == END_ELEMENT || eventType == END_DOCUMENT) {
            return null;
        }
        if (eventType != START_ELEMENT) {
            throw new XMLParseException("start of element");
        }
        assertLocalName("changeset");

        var id = -1L;
        Instant created = null;
        Instant closed = null;
        String user = null;
        var userId = -1;
        var minLon = Double.NaN;
        var minLat = Double.NaN;
        var maxLon = Double.NaN;
        var maxLat = Double.NaN;

        var numChanges = -1;
        var commentsCount = -1;
        for (var i = 0; i < reader.getAttributeCount(); i++) {
            var attrName = reader.getAttributeLocalName(i);
            var attrValue = reader.getAttributeValue(i);
            switch (attrName) {
                case "id" -> id = Long.parseLong(attrValue);
                case "created_at" -> created = Instant.parse(attrValue);
                case "closed_at" -> closed = Instant.parse(attrValue);
                case "user" -> user = attrValue;
                case "uid" -> userId = Integer.parseInt(attrValue);
                case "num_changes" -> numChanges = Integer.parseInt(attrValue);
                case "comments_count" -> commentsCount = Integer.parseInt(attrValue);
                case "min_lon" -> minLon = Double.parseDouble(attrValue);
                case "min_lat" -> minLat = Double.parseDouble(attrValue);
                case "max_lon" -> maxLon = Double.parseDouble(attrValue);
                case "max_lat" -> maxLat = Double.parseDouble(attrValue);
                case "open" -> {
                    // ignore the open flag
                }
                default -> throw new XMLParseException("unknown attribute: " + attrName);
            }
        }

        var tags = new HashMap<String, String>();
        while ((eventType = nextEvent()) == START_ELEMENT) {
            assertLocalName("tag");
            var key = reader.getAttributeValue(null, "k");
            var value = reader.getAttributeValue(null, "v");
            if (key == null || value == null) {
                throw new XMLParseException("tag! missing key(%s) or value(%s)".formatted(key, value));
            }
            tags.put(key, value);
            eventType = reader.nextTag();
            if (eventType != END_ELEMENT) {
                throw new XMLParseException("unclosed tag");
            }
        }
        if (eventType != END_ELEMENT) {
            throw new XMLParseException("expect tag end but got %s".formatted(eventType));
        }
        return factory.apply(id, created, closed, user, userId, minLon, minLat, maxLon, maxLat, numChanges, commentsCount, tags);
    }


    private int nextEvent() throws XMLStreamException {
        while (true) {
            var event = reader.next();
            switch (event) {
                case START_ELEMENT, END_ELEMENT, END_DOCUMENT:
                    return event;
                case SPACE, COMMENT, PROCESSING_INSTRUCTION, CDATA, CHARACTERS:
                    break; // skip
                default:
                    throw new XMLStreamException("Received event %s, instead of START_ELEMENT or END_ELEMENT or END_DOCUMENT.".formatted(event));
            }
        }
    }

    private void assertStartElement(String name) throws XMLParseException, XMLStreamException {
        var eventType = nextEvent();
        if (eventType != START_ELEMENT) {
            throw new XMLParseException(
                    "expecting start of element (" + START_ELEMENT + ") but got " + eventType);
        }
        assertLocalName(name);
    }

    private void assertLocalName(String name) throws XMLParseException {
        var localName = reader.getLocalName();
        if (!name.equals(localName)) {
            throw new XMLParseException("expecting tag %s but got %s".formatted(name, localName));
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return next != null || ((next = computeNext()) != null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public T peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public T next() {
        var peek = peek();
        next = null;
        return peek;
    }
}
