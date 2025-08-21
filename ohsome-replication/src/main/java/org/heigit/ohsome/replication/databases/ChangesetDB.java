package org.heigit.ohsome.replication.databases;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.osm.changesets.ChangesetDb;
import org.heigit.ohsome.osm.changesets.ChangesetHashtags;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.replication.parser.ChangesetParser;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.postgresql.util.HStoreConverter;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class ChangesetDB {
    private static final HikariConfig config = new HikariConfig();
    private final HikariDataSource dataSource;

    public <T> Map<Long, T> changesets(Set<Long> ids, Changesets.Factory<T> factory) throws Exception {
        return getterDb.changesets(ids, factory);
    }

    private final ChangesetDb getterDb;

    public ChangesetDB(String connectionString) {
        config.setJdbcUrl(connectionString);
        dataSource = new HikariDataSource(config);
        getterDb = new ChangesetDb(dataSource);
    }

    public ReplicationState getLocalState() {
        try (var conn = dataSource.getConnection();
             var pstmt = conn.prepareStatement("SELECT last_sequence, last_timestamp FROM osm_changeset_state")
        ) {
            var results = pstmt.executeQuery();
            if (results.next()) {
                return new ReplicationState(results.getTimestamp(2).toInstant(), results.getInt(1));
            } else {
                throw new RuntimeException("No state in changesetDB");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateState(ReplicationState state) {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("UPDATE osm_changeset_state SET last_sequence=?, last_timestamp=?")
        ) {
            pstmt.setInt(1, state.getSequenceNumber());
            pstmt.setTimestamp(2, Timestamp.from(state.getTimestamp()));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void upsertChangesets(List<ChangesetParser.Changeset> changesets) {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("""
                         MERGE INTO osm_changeset as cs
                         USING (
                            VALUES(
                             ?,
                             ?,
                             ?::timestamp,
                             ?,
                             ?,
                             ?,
                             ?,
                             ?::timestamp,
                             ?,
                             ?,
                             ?,
                             ?,
                             ST_MakeEnvelope(?, ?, ?, ?),
                             ?
                            )
                         ) upserts (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags, geom, hashtags)
                         ON cs.id = upserts.id
                         WHEN matched THEN
                           UPDATE SET
                               min_lat = upserts.min_lat,
                               max_lat = upserts.max_lat,
                               min_lon = upserts.min_lon,
                               max_lon = upserts.max_lon,
                               closed_at = upserts.closed_at,
                               open = upserts.open,
                               num_changes = upserts.num_changes,
                               tags = upserts.tags,
                               geom = upserts.geom,
                               hashtags = upserts.hashtags
                         WHEN NOT matched THEN
                           INSERT (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags, geom, hashtags)
                           VALUES (upserts.id, upserts.user_id, upserts.created_at, upserts.min_lat, upserts.max_lat, upserts.min_lon, upserts.max_lon, upserts.closed_at, upserts.open, upserts.num_changes, upserts.user_name, upserts.tags, upserts.geom, upserts.hashtags);
                        """
                )
        ) {
            for (var changeset : changesets) {
                pstmt.setLong(1, changeset.id);
                pstmt.setLong(2, changeset.uid);
                pstmt.setTimestamp(3, Timestamp.from(changeset.getCreatedAt()));

                pstmt.setBigDecimal(4, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.minLat, 0.)));
                pstmt.setBigDecimal(5, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.maxLat, 0.)));
                pstmt.setBigDecimal(6, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.minLon, 0.)));
                pstmt.setBigDecimal(7, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.maxLon, 0.)));

                if (Objects.isNull(changeset.getClosedAt())) {
                    pstmt.setTimestamp(8, null);
                    pstmt.setBoolean(9, false);
                } else {
                    pstmt.setTimestamp(8, Timestamp.from(changeset.getClosedAt()));
                    pstmt.setBoolean(9, true);
                }

                pstmt.setInt(10, changeset.numChanges);
                pstmt.setString(11, changeset.userName);

                PGobject hstoreTags = new PGobject();
                hstoreTags.setType("hstore");
                hstoreTags.setValue(HStoreConverter.toString(changeset.tagsAsMap()));

                pstmt.setObject(12, hstoreTags);

                pstmt.setBigDecimal(13, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.minLat, 0.)));
                pstmt.setBigDecimal(14, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.minLon, 0.)));
                pstmt.setBigDecimal(15, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.maxLat, 0.)));
                pstmt.setBigDecimal(16, BigDecimal.valueOf(Objects.requireNonNullElse(changeset.maxLon, 0.)));

                pstmt.setArray(17, conn.createArrayOf("VARCHAR", ChangesetHashtags.hashTags(changeset.tagsAsMap()).toArray()));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<Long> getOpenChangesetsOlderThanTwoHours() {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("SELECT id FROM osm_changeset where created_at < now() - interval '2 hours' and open")
        ) {
            var results = pstmt.executeQuery();
            List<Long> ids = new ArrayList<>();
            while (results.next()) {
                ids.add(results.getLong(1));
            }
            return ids;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
