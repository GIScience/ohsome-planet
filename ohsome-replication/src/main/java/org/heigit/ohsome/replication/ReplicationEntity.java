package org.heigit.ohsome.replication;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMMember;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.util.io.Output;

import java.time.Instant;
import java.util.*;

import static java.nio.ByteBuffer.*;
import static org.heigit.ohsome.util.io.Input.fromBuffer;

public class ReplicationEntity {

    public static void serialize(OSMNode node, Output output) {
        serializeEntity(node, output);
        output.writeS64(Math.round(node.lon() * 1_0000000L));
        output.writeS64(Math.round(node.lat() * 1_0000000L));
    }

    public static OSMNode deserializeNode(long id, byte[] bytes) {
        var input = fromBuffer(wrap(bytes));
        var entityInfo = deserializeEntity(input);
        var lon = input.readS64() / 1_0000000.0;
        var lat = input.readS64() / 1_0000000.0;
        return new OSMNode(id,
                entityInfo.version(),
                entityInfo.timestamp(),
                -1, -1, "", true,
                entityInfo.tags(),
                lon, lat);
    }


    public static void serialize(OSMWay way, Output output) {
        serializeEntity(way, output);
        output.writeU32(way.minorVersion());
        output.writeU32(way.edits());
        output.writeU32(way.refs().size());
        var lastRef = 0L;
        for (var ref : way.refs()) {
            output.writeS64(ref - lastRef);
            lastRef = ref;
        }
    }

    public static OSMWay deserializeWay(long id, byte[] bytes) {
        var input = fromBuffer(wrap(bytes));
        var entityInfo = deserializeEntity(input);
        var minorVersion = input.readU32();
        var edits = input.readU32();
        var refsSize = input.readU32();
        var refs = new ArrayList<Long>(refsSize);
        var refId = 0L;
        for (var i = 0; i < refsSize; i++) {
            refId = refId + input.readS64();
            refs.add(refId);
        }
        return new OSMWay(id,
                entityInfo.version(),
                entityInfo.timestamp(),
                -1, -1, "", true,
                entityInfo.tags(),
                refs,
                minorVersion, edits, null, null);
    }

    public static void serialize(OSMRelation relation, Output output) {
        serializeEntity(relation, output);
        output.writeU32(relation.minorVersion());
        output.writeU32(relation.edits());
        output.writeU32(relation.members().size());
        var lastId = 0L;
        for (var member : relation.members()) {
            output.writeU32(member.type().id());
            output.writeS64(member.id() - lastId);
            output.writeUTF8(member.role());
            lastId = member.id();
        }
    }

    public static OSMRelation deserializeRelation(long id, byte[] bytes) {
        var input = fromBuffer(wrap(bytes));
        var entityInfo = deserializeEntity(input);
        var minorVersion = input.readU32();
        var edits = input.readU32();
        var memberSize =  input.readU32();
        var members = new ArrayList<OSMMember>(memberSize);
        var memId = 0L;
        for (var i = 0; i < memberSize; i++) {
            var typeId =  input.readU32();
            memId += input.readS64();
            var role = input.readUTF8();
            members.add(new OSMMember(OSMType.parseType(typeId), memId, role));
        }

        return new OSMRelation(id,
                entityInfo.version(),
                entityInfo.timestamp(),
                -1, -1, "", true,
                entityInfo.tags(),
                members,
                minorVersion, edits);
    }


    public static void serialize(Set<Long> set, Output output) {
        var last = 0L;
        for (var id : sorted(set)) {
            output.writeU64(id - last);
            last = id;
        }
    }






    public static Set<Long> deserializeSet(long id, byte[] bytes) {
        var input = fromBuffer(wrap(bytes));
        var set = new HashSet<Long>();
        var last = 0L;
        while (input.hasRemaining()) {
            var delta = input.readU64();
            if (delta == 0) {
                last = 0;
                continue;
            }
            last = last + delta;
            set.add(last);
        }
        return set;
    }


    private record EntityInfo(Instant timestamp, int version, Map<String, String> tags) {
    }

    private static void serializeEntity(OSMEntity entity, Output output) {
        output.writeU64(entity.timestamp().getEpochSecond());
//        output.writeU64(entity.changeset());
        output.writeU32(entity.version());
//        output.writeU32(entity.userId());
//        output.writeUTF8(entity.user());
        output.writeU32(entity.tags().size());
        for (var tag : entity.tags().entrySet()) {
            output.writeUTF8(tag.getKey());
            output.writeUTF8(tag.getValue());
        }
    }

    private static EntityInfo deserializeEntity(Input input) {
        var timestamp = input.readU64();
//        var changeset = input.readU64();
        var version = input.readU32();
        var tagsSize = input.readU32();
        var tags = new HashMap<String, String>();
        for (var i = 0; i < tagsSize; i++) {
            tags.put(input.readUTF8(), input.readUTF8());
        }
        return new EntityInfo(Instant.ofEpochSecond(timestamp), version, tags);
    }

    private static SortedSet<Long> sorted(Set<Long> set) {
        if (set instanceof SortedSet<Long> sortedSet) {
            return sortedSet;
        }
        return new TreeSet<>(set);
    }

}
