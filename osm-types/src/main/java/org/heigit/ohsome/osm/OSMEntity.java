package org.heigit.ohsome.osm;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public sealed interface OSMEntity {

    long id();

    OSMType type();

    default OSMId osmId() {
        return new OSMId(type(), id());
    }

    int version();

    int minorVersion();

    int edits();

    Instant timestamp();

    long changeset();

    int userId();

    String user();

    boolean visible();

    Map<String, String> tags();

    List<OSMMember> members();


    record OSMNode(long id, int version, Instant timestamp, long changeset, int userId, String user,
                   boolean visible,
                   Map<String, String> tags, double lon, double lat) implements OSMEntity {

        @Override
        public OSMType type() {
            return OSMType.NODE;
        }

        @Override
        public int minorVersion() {
            return 0;
        }

        @Override
        public int edits() {
            return version;
        }

        @Override
        public List<OSMMember> members() {
            return Collections.emptyList();
        }
    }

    record OSMWay(long id, int version, Instant timestamp, long changeset, int userId, String user,
                  boolean visible,
                  Map<String, String> tags, List<Long> refs, int minorVersion, int edits, List<Long> lons,
                  List<Long> lats) implements OSMEntity {

        public OSMWay(long id, int version, Instant timestamp, long changeset, int userId, String user,
                      boolean visible,
                      Map<String, String> tags, List<Long> refs) {
            this(id, version, timestamp, changeset, userId, user, visible, tags, refs, 0, 0, null, null);
        }

        @Override
        public OSMType type() {
            return OSMType.WAY;
        }

        public OSMWay withMinorAndEdits(int minorVersion, int edits) {
            return new OSMWay(id, version, timestamp, changeset, userId, user, visible, tags, refs, minorVersion, edits, null, null);
        }

        @Override
        public List<OSMMember> members() {
            return refs.stream().map(ref -> new OSMMember(OSMType.NODE, ref, "")).toList();
        }
    }

    record OSMRelation(long id, int version, Instant timestamp, long changeset, int userId,
                       String user, boolean visible,
                       Map<String, String> tags, List<OSMMember> members, int minorVersion, int edits) implements OSMEntity {

        public OSMRelation(long id, int version, Instant timestamp, long changeset, int userId,
                           String user, boolean visible,
                           Map<String, String> tags, List<OSMMember> members) {
            this(id, version, timestamp, changeset, userId, user, visible, tags, members, 0, 0);
        }

        @Override
        public OSMType type() {
            return OSMType.RELATION;
        }

        public OSMRelation withMinorAndEdits(int minorVersion, int edits) {
            return new OSMRelation(id, version, timestamp, changeset, userId, user, visible, tags, members, minorVersion, edits);
        }

    }

}
