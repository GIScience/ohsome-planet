package org.heigit.ohsome.osm;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public sealed interface OSMEntity {

    long id();

    OSMType type();

    int version();

    int minorVersion();

    int edits();

    Instant timestamp();

    long changeset();

    int userId();

    String user();

    boolean visible();

    Map<String, String> tags();

    int memberSize();

    interface MembersFunction {
        void member(OSMType type, long id, String role);
    }

    void members(MembersFunction consumer);

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
        public int memberSize() {
            return 0;
        }

        @Override
        public void members(MembersFunction consumer) {
            // no members
        }
    }

    record OSMWay(long id, int version, Instant timestamp, long changeset, int userId, String user,
                  boolean visible,
                  Map<String, String> tags, long[] refs, int minorVersion, int edits, List<Long> lons,
                  List<Long> lats) implements OSMEntity {

        public OSMWay(long id, int version, Instant timestamp, long changeset, int userId, String user,
                      boolean visible,
                      Map<String, String> tags, long[] refs) {
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
        public int memberSize() {
            return refs.length;
        }


        @Override
        public void members(MembersFunction consumer) {
            for (var ref : refs) {
                consumer.member(OSMType.NODE, ref, "");
            }
        }
    }


    record OSMRelation(long id, int version, Instant timestamp, long changeset, int userId,
                        String user, boolean visible, Map<String, String> tags,
                        OSMType[] memberTypes, long[] memberIds, String[] memberRoles,
                        int minorVersion, int edits) implements OSMEntity {

        public  OSMRelation(long id, int version, Instant timestamp, long changeset, int userId,
                             String user, boolean visible, Map<String, String> tags,
                             OSMType[] memberTypes, long[] memberIds, String[] memberRoles) {
            this(id, version, timestamp, changeset, userId, user, visible, tags, memberTypes, memberIds, memberRoles, 0,0);
        }

        @Override
        public OSMType type() {
            return OSMType.RELATION;
        }

        @Override
        public int memberSize() {
            return memberTypes.length;
        }

        @Override
        public void members(MembersFunction consumer) {
            for (var i = 0; i < memberTypes.length; i++) {
                consumer.member(memberTypes[i], memberIds[i], memberRoles[i]);
            }
        }

        public OSMRelation withMinorAndEdits(int minorVersion, int edits) {
            return new OSMRelation(id, version, timestamp, changeset, userId, user, visible, tags, memberTypes, memberIds, memberRoles, minorVersion, edits);
        }
    }

}
