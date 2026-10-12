package org.heigit.ohsome.changesets;

import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.changesets.ChangesetDBTest.TestChangeset2;
import org.heigit.ohsome.osm.changesets.ChangesetDb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


// using inheritance to trigger side-effects to access stored data
// in order not to change prod interface Factory<T>
class TestSupportingChangeset_Db extends ChangesetDb {

    Map<Long, TestChangeset2> collectedChangesets = new HashMap<>();


    TestSupportingChangeset_Db(HikariDataSource dataSource) {
        super(dataSource);
    }

    TestChangeset2 getChangeset(long id) {
        return this.collectedChangesets.get(id);
    }


    @Override
    public String createSelectChangesetsQuery() {
        return "select id, created_at, closed_at, tags, hashtags, upserted_at from %s where id = any(?)";
    }


    @Override
    public void postProcessInstance(ResultSet rst, long id, Instant createdAt, Instant closedAt, Map<String, String> tags, List<String> hashTags, String editor) throws SQLException {
        var upsertedAt = rst.getTimestamp(6).toInstant();

        // grab instance here, augment to desired type, and pass on original instance
        var result = new TestChangeset2(id, createdAt, closedAt, tags, hashTags, editor, upsertedAt);
        this.collectedChangesets.put(id, result);
    }

}
