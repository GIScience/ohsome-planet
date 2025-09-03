package org.heigit.ohsome.replication.databases;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.osm.changesets.ChangesetDb;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.postgresql.PGConnection;
import org.postgresql.util.HStoreConverter;
import org.postgresql.util.PGobject;
import reactor.core.publisher.Mono;

import java.io.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;

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

    public ReplicationState getLocalState() throws NoSuchElementException {
        try (var conn = dataSource.getConnection();
             var pstmt = conn.prepareStatement("SELECT last_sequence, last_timestamp FROM osm_changeset_state")
        ) {
            var results = pstmt.executeQuery();
            if (results.next()) {
                return new ReplicationState(results.getTimestamp(2).toInstant(), results.getInt(1));
            } else {
                throw new NoSuchElementException("No state in changesetDB");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Instant getMaxLocalTimestamp() {
        try (var conn = dataSource.getConnection();
             var pstmt = conn.prepareStatement("SELECT max(created_at) FROM osm_changeset")
        ) {
            var results = pstmt.executeQuery();
            if (results.next()) {
                return results.getTimestamp(1).toInstant();
            } else {

                throw new RuntimeException("No data in changesetDB");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


    public void updateState(ReplicationState state) {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("""
                        MERGE INTO osm_changeset_state as ocs
                        USING (VALUES(0, ?, ?::timestamp)) upserts (id, last_sequence, last_timestamp)
                        ON ocs.id = upserts.id
                         WHEN matched THEN
                           UPDATE SET
                               last_sequence = upserts.last_sequence,
                               last_timestamp = upserts.last_timestamp
                         WHEN NOT matched THEN
                           INSERT (id, last_sequence, last_timestamp)
                           VALUES (upserts.id, upserts.last_sequence, upserts.last_timestamp);
                        """
                )
        ) {
            pstmt.setInt(1, state.getSequenceNumber());
            pstmt.setTimestamp(2, Timestamp.from(state.getTimestamp()));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getMaxConnections() {
        try {
            return dataSource.getConnection().getMetaData().getMaxConnections();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void upsertChangesets(List<OSMChangeset> changesets) {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("""
                                INSERT INTO osm_changeset (
                                    id,
                                    user_id,
                                    created_at,
                                    closed_at,
                                    open,
                                    num_changes,
                                    user_name,
                                    tags
                                )
                                VALUES (?, ?, ?::timestamp, ?::timestamp, ?, ?, ?, ?)
                                ON CONFLICT (id) DO UPDATE
                                SET
                                    closed_at = EXCLUDED.closed_at,
                                    open = EXCLUDED.open,
                                    num_changes = EXCLUDED.num_changes,
                                    tags = EXCLUDED.tags
                                WHERE NOT EXCLUDED.open;
                      """
                )
        ) {
            for (var changeset : changesets) {
                pstmt.setLong(1, changeset.id());
                pstmt.setLong(2, changeset.userId());
                pstmt.setTimestamp(3, Timestamp.from(changeset.getCreatedAt()));

                if (changeset.isOpen()) {
                    pstmt.setTimestamp(4, null);
                } else {
                    pstmt.setTimestamp(4, Timestamp.from(changeset.getClosedAt()));
                }
                pstmt.setBoolean(5, changeset.isOpen());

                pstmt.setInt(6, changeset.numChanges());
                pstmt.setString(7, changeset.user());

                PGobject hstoreTags = new PGobject();
                hstoreTags.setType("hstore");
                hstoreTags.setValue(HStoreConverter.toString(changeset.tags()));

                pstmt.setObject(8, hstoreTags);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }


    static final String NULL = "";

    public Mono<String> changesets2CSV(List<OSMChangeset> changesets) {
        return Mono.fromCallable(() -> {
            var stringWriter = new StringWriter();
            try (var csvWriter = new PrintWriter(stringWriter)) {
                for (var changeset : changesets) {
                    var line = String.format(
                            "%d\t%s\t%s\t%s\t%b\t%d\t%s\t%s%n",
                            changeset.id(),
                            changeset.userId(),
                            Timestamp.from(changeset.getCreatedAt()),
                            changeset.getClosedAt() == null ? NULL : Timestamp.from(changeset.getClosedAt()),
                            changeset.getClosedAt() == null,
                            changeset.numChanges(),
                            escapeCsv(changeset.user()),
                            escapeCsv(HStoreConverter.toString(changeset.tags()))
                    );
                    csvWriter.write(line);
                }
            }
            return stringWriter.toString();
        });
    }

    public static String escapeCsv(String value) {
        if (value == null || value.isEmpty()) {
            return NULL;
        }
        return "\"" + value.replace("\"", "\"\"").replace("\t", " ") + "\"";
    }

    public Mono<Void> bulkInsertChangesets(String changesetCSVString) {
        return Mono.fromRunnable(() -> {
            try (var conn = dataSource.getConnection()) {
                var pgConn = conn.unwrap(PGConnection.class);
                var copyManager = pgConn.getCopyAPI();

                try (Reader reader = new StringReader(changesetCSVString)) {
                    copyManager.copyIn(
                            "COPY osm_changeset (id, user_id, created_at, closed_at, open, num_changes, user_name, tags) " +
                                    "FROM STDIN WITH CSV DELIMITER '\t'",
                            reader
                    );
                }
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        });
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
            System.out.println("Got " + ids.size() + " unclosed changesets older than 2 hours from database");
            return ids;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
