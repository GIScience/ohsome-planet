package org.heigit.ohsome.replication;

import com.google.common.io.MoreFiles;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    static void setUp() {
        postgresContainer.withInitScripts("setupDB/setupChangesetDB.sql", "setupDB/initializeDataForReplicationUpdate.sql");
        postgresContainer.start();
        dbUrl = postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword();
    }

    @AfterAll
    static void tearDown() {
        postgresContainer.stop();
    }


    @Test
    void testUpdateOnlyChangesets() throws Exception {
        var replicationChangesetUrl = RESOURCE_PATH.resolve("replication/changesets").toUri().toURL().toString();
        ReplicationManager.update(dbUrl, replicationChangesetUrl, false);

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

        Files.copy(ohsomePlanetPath.resolve("default-state.txt"), ohsomePlanetPath.resolve("state.txt"), StandardCopyOption.REPLACE_EXISTING);
        if (Files.exists(out)) {
            MoreFiles.deleteRecursively(out);
        }

        ReplicationManager.update(ohsomePlanetPath, out, replicationElementsUrl, false);

        var localStateAfterUpdate = ContributionStateManager.loadLocalState(ohsomePlanetPath.resolve("state.txt"));
        assertEquals(6824842, localStateAfterUpdate.getSequenceNumber());
    }

    @Test
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
            ReplicationManager.update(ohsomePlanetPath, out, replicationElementsUrl, dbUrl, replicationChangesetUrl, false, false, false);

            var localChangesetStateAfterUpdate = changesetDb.getLocalState();
            assertEquals(6737400, localChangesetStateAfterUpdate.getSequenceNumber());

            var localContributionStateAfterUpdate = ContributionStateManager.loadLocalState(ohsomePlanetPath.resolve("state.txt"));
            assertEquals(6824842, localContributionStateAfterUpdate.getSequenceNumber());
        }
    }
}
