package org.heigit.ohsome.changesets;

import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.osm.changesets.ChangesetDb;
import org.jspecify.annotations.NonNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestSupportingChangesetDb extends ChangesetDb {

    public TestSupportingChangesetDb(HikariDataSource dataSource) {
        super(dataSource);
    }

    @Override
    public <T> Map<Long, T> changesets(Set<Long> ids, String table, Factory<T> factory) throws Exception {
        System.out.println("#########");
        return super.changesets(ids, table, factory);
    }

    @Override
    public @NonNull String createSelectChangesetsQuery() {
        return "select id, created_at, closed_at, tags, hashtags, upserted_at from %s where id = any(?)";
    }

    @Override
    public <T> T createChangesetInstance(ResultSet rst, Factory<T> factory, long id, Instant createdAt, Instant closedAt, Map<String, String> tags, List<String> hashTags, String editor) throws SQLException {
        var upsertedAt = rst.getTimestamp(6).toInstant();
        return factory.apply(id, createdAt, closedAt, tags, hashTags, editor, upsertedAt);
    }

}
