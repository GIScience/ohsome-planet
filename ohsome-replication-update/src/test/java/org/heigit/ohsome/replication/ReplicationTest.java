package org.heigit.ohsome.replication;

import com.google.common.io.MoreFiles;
import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

/**
 * Unit test for simple App.
 */
class ReplicationTest {
    private static final Path RESOURCE_PATH = Path.of("src/test/resources/");

    @Container
    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:17-3.5")
                    .asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");
    private static String dbUrl;

    @BeforeAll
    static void setup() {
        postgresContainer.withInitScripts("setupChangesetDB.sql");
        postgresContainer.start();
        dbUrl = postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword();
    }

    @BeforeEach
    void setUp() throws SQLException, IOException {
        var config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        for (var script : new String[]{"setupDB/initializeDataForReplication.sql"}){
            try (var datasource = new HikariDataSource(config);
                 var conn = datasource.getConnection();
                 var st = conn.prepareStatement(
                         Resources.toString(Resources.getResource(script), StandardCharsets.UTF_8)
                 )
            ) {
                st.execute();
            }
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (var db = new ChangesetDB(dbUrl)) {
            db.truncateChangesetTables();
        }
    }

    @AfterAll
    static void teardown() {
        postgresContainer.stop();
    }

    @Test
    void testUpdateOnlyChangesets() throws Exception {
        var replicationChangesetUrl = RESOURCE_PATH.resolve("replication/changesets").toUri().toURL().toString();
        ReplicationManager.updateChangesets(dbUrl, replicationChangesetUrl, false);

        try (var changesetDb = new ChangesetDB(dbUrl)) {
            var localStateAfterUpdate = changesetDb.getLocalState();
            assertEquals(6737400, localStateAfterUpdate.getSequenceNumber());
        }
    }

    @Test
    void testUpdateOnlyContributions() throws Exception {
        var ohsomePlanetPath = RESOURCE_PATH.resolve("ohsome-planet");
        var out = RESOURCE_PATH.resolve("out");
        var replicationElementsUrl = RESOURCE_PATH.resolve("replication/minute").toUri().toURL().toString();

        var state = ReplicationState.read(ohsomePlanetPath.resolve("default-state.txt"));
        var stateData = state.toBytes(replicationElementsUrl);
        Files.write(ohsomePlanetPath.resolve("state.txt"), stateData);
        if (Files.exists(out)) {
            MoreFiles.deleteRecursively(out);
        }

        ReplicationManager.updateContributions(ohsomePlanetPath, out, false);

        var localStateAfterUpdate = ContributionStateManager.loadLocalState(ohsomePlanetPath.resolve("state.txt"));
        assertEquals(6824842, localStateAfterUpdate.getSequenceNumber());
    }

    @Test
    @Disabled
    void testUpdateBothContributionsAndChangesets() throws Exception {
        var ohsomePlanetPath = RESOURCE_PATH.resolve("ohsome-planet");
        var out = RESOURCE_PATH.resolve("out");
        var replicationElementsUrl = RESOURCE_PATH.resolve("replication/minute").toUri().toURL().toString();
        Files.copy(ohsomePlanetPath.resolve("default-state.txt"), ohsomePlanetPath.resolve("state.txt"), StandardCopyOption.REPLACE_EXISTING);
        if (Files.exists(out)) {
            MoreFiles.deleteRecursively(out);
        }

        var replicationChangesetUrl = RESOURCE_PATH.resolve("replication/changesets").toUri().toURL().toString();

        try (var changesetDb = new ChangesetDB(dbUrl)) {
            assertThrowsExactly(NoSuchElementException.class, changesetDb::getLocalState);
            ReplicationManager.update(ohsomePlanetPath, out, dbUrl, replicationChangesetUrl, false);

            var localChangesetStateAfterUpdate = changesetDb.getLocalState();
            assertEquals(6737400, localChangesetStateAfterUpdate.getSequenceNumber());

            var localContributionStateAfterUpdate = ContributionStateManager.loadLocalState(ohsomePlanetPath.resolve("state.txt"));
            assertEquals(6824842, localContributionStateAfterUpdate.getSequenceNumber());
        }
    }
}
