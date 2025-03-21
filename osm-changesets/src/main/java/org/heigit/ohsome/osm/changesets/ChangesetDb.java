package org.heigit.ohsome.osm.changesets;

import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ChangesetDb implements Changesets {

    private final HikariDataSource dataSource;

    public ChangesetDb(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) throws Exception {
        try (var conn = dataSource.getConnection();
             var pstmt = conn.prepareStatement("select id, created_at, closed_at, tags, num_changes from osm_changeset where id = any(?)");
             var array = ClosableSqlArray.createArray(conn, "int", ids)) {
            pstmt.setArray(1, array.array());
            var map = Maps.<Long, T>newHashMapWithExpectedSize(ids.size());
            try (var rst = pstmt.executeQuery()) {
                while (rst.next()) {
                    var id = rst.getLong(1);
                    var createdAt = Optional.ofNullable(rst.getTimestamp(2)).map(Timestamp::toInstant).orElse(null);
                    var closedAt = Optional.ofNullable(rst.getTimestamp(3)).map(Timestamp::toInstant).orElse(null);
                    @SuppressWarnings("unchecked")
                    var tags = (Map<String, String>) rst.getObject(4);
                    var numChanges = rst.getInt(5);
                    var hashTags = ChangesetHashtags.hashTags(tags);
                    var editor = tags.get("created_by");
                    map.put(id, factory.apply(id, createdAt, closedAt, tags, hashTags, editor, numChanges));
                }
                return map;
            }
        }
    }

    private record ClosableSqlArray(Array array) implements AutoCloseable {

        public static <T> ClosableSqlArray createArray(Connection conn, String typeName, Collection<T> elements) throws SQLException {
            var array = conn.createArrayOf(typeName, elements.toArray());
            return new ClosableSqlArray(array);
        }

        @Override
        public void close() throws Exception {
            array.free();
        }
    }

}
