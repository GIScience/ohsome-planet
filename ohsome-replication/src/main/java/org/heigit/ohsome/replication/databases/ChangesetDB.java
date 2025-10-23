package org.heigit.ohsome.replication.databases;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.osm.changesets.ChangesetDb;
import org.heigit.ohsome.osm.changesets.ChangesetHashtags;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.postgresql.PGConnection;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;

public class ChangesetDB implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ChangesetDB.class);
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

    public ReplicationState getLocalState() throws NoSuchElementException, SQLException {
        try (var conn = dataSource.getConnection();
             var pstmt = conn.prepareStatement("SELECT last_sequence, last_timestamp FROM changeset_state")
        ) {
            var results = pstmt.executeQuery();
            if (results.next()) {
                return new ReplicationState(results.getTimestamp(2).toInstant(), results.getInt(1));
            } else {
                throw new NoSuchElementException("No state in changesetDB");
            }
        }
    }

    public Instant getMaxLocalTimestamp() throws SQLException {
        try (var conn = dataSource.getConnection();
             var pstmt = conn.prepareStatement("SELECT max(created_at) FROM changesets")
        ) {
            var results = pstmt.executeQuery();
            if (results.next()) {
                return results.getTimestamp(1).toInstant();
            } else {
                throw new NoSuchElementException("No data in changesetDB, run changeset command first to initialize changesetDB");
            }
        }
    }


    public void updateState(ReplicationState state) throws SQLException {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("""
                        INSERT INTO changeset_state
                        VALUES(0, ?, ?::timestamp)
                        ON CONFLICT (id) DO UPDATE
                        SET
                            last_sequence = EXCLUDED.last_sequence,
                            last_timestamp = EXCLUDED.last_timestamp
                        """
                )
        ) {
            pstmt.setInt(1, state.getSequenceNumber());
            pstmt.setTimestamp(2, Timestamp.from(state.getTimestamp()));
            pstmt.executeUpdate();
            logger.debug("State updated to {}", state.getSequenceNumber());
        }
    }

    public int getMaxConnections() {
        return config.getMaximumPoolSize();
    }

    public void upsertChangesets(List<OSMChangeset> changesets) throws JsonProcessingException, SQLException {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("""
                                  INSERT INTO changesets (
                                      changeset_id,
                                      user_id,
                                      created_at,
                                      closed_at,
                                      open,
                                      user_name,
                                      tags,
                                      editor,
                                      hashtags
                                  )
                                  VALUES (?, ?, ?::timestamp, ?::timestamp, ?, ?, ?, ?, ?)
                                  ON CONFLICT (changeset_id) DO UPDATE
                                  SET
                                      closed_at = EXCLUDED.closed_at,
                                      open = EXCLUDED.open,
                                      tags = EXCLUDED.tags,
                                      hashtags = EXCLUDED.hashtags,
                                      editor = EXCLUDED.editor
                                  WHERE NOT EXCLUDED.open;
                        """
                )
        ) {
            ObjectMapper objectMapper = new ObjectMapper();
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


                pstmt.setString(6, changeset.user());

                var tags = changeset.tags();
                PGobject jsonTags = new PGobject();
                jsonTags.setType("jsonb");
                jsonTags.setValue(objectMapper.writeValueAsString(tags));
                pstmt.setObject(7, jsonTags);

                pstmt.setString(8, tags.get("created_by"));
                pstmt.setArray(9, conn.createArrayOf("varchar", ChangesetHashtags.hashTags(tags).toArray()));
                pstmt.addBatch();
            }
            logger.debug("Trying to upsert {} changesets",  changesets.size());
            pstmt.executeBatch();
            logger.debug("Successfully upserted {} changesets",  changesets.size());
        }
    }


    static final String NULL = "";

    public byte[] changesets2CSV(List<OSMChangeset> changesets) throws IOException {
        try (var out = new ByteArrayOutputStream();
             var writer = new PrintWriter(out)) {
            ObjectMapper objectMapper = new ObjectMapper();
            for (var changeset : changesets) {
                var tags = changeset.tags();
                var line = String.format(
                        "%d\t%s\t%s\t%s\t%b\t%s\t%s\t%s\t%s%n",
                        changeset.id(),
                        changeset.userId(),
                        Timestamp.from(changeset.getCreatedAt()),
                        changeset.getClosedAt() == null ? NULL : Timestamp.from(changeset.getClosedAt()),
                        changeset.getClosedAt() == null,
                        changeset.user() == null ? "\"\"" : escapeCsv(changeset.user()),
                        escapeCsv(objectMapper.writeValueAsString(tags)),
                        escapeCsv("{" + String.join(",", ChangesetHashtags.hashTags(tags)) + "}"),
                        escapeCsv(tags.get("created_by"))
                );
                writer.write(line);
            }
            writer.flush();
            return out.toByteArray();
        }
    }


    public static String escapeCsv(String value) {
        if (value == null || value.isEmpty()) {
            return NULL;
        }
        return "\"" + value.replace("\"", "\"\"").replace("\t", " ") + "\"";
    }

    public void bulkInsertChangesets(byte[] changesetCSV) throws SQLException, IOException {
        try (var conn = dataSource.getConnection()) {
            var pgConn = conn.unwrap(PGConnection.class);
            var copyManager = pgConn.getCopyAPI();

            try (var stream = new ByteArrayInputStream(changesetCSV)) {
                copyManager.copyIn(
                        "COPY changesets (changeset_id, user_id, created_at, closed_at, open, user_name, tags, hashtags, editor)" +
                                "FROM STDIN WITH CSV DELIMITER '\t'",
                        stream
                );
            }
        }
    }

    public List<Long> getOpenChangesetsOlderThanTwoHours() {
        try (
                var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement("SELECT changeset_id FROM changesets where created_at < now() - interval '2 hours' and open")
        ) {
            var results = pstmt.executeQuery();
            List<Long> ids = new ArrayList<>();
            while (results.next()) {
                ids.add(results.getLong(1));
            }
            logger.info("Got " + ids.size() + " unclosed changesets older than 2 hours from database");
            return ids;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
