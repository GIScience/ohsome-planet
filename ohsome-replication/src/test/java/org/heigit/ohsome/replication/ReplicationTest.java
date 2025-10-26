package org.heigit.ohsome.replication;

import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
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
        postgresContainer.withInitScript("setupDB/setupReplicationUpdate.sql");
        postgresContainer.start();
        dbUrl = postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword();
    }

    @AfterAll
    static void tearDown() {
        postgresContainer.stop();
    }


    @Test
    void testReplication() throws Exception {
        var replicationChangesetUrl = RESOURCE_PATH.resolve("replication/changesets").toUri().toURL().toString();
        var replicationElementsUrl = RESOURCE_PATH.resolve("replication/minute").toUri().toURL().toString();
        var ohsomePlanetPath = RESOURCE_PATH.resolve("ohsome-planet");
        var out = RESOURCE_PATH.resolve("out");


        try (var changesetDb = new ChangesetDB(dbUrl)) {
            assertThrowsExactly(NoSuchElementException.class, changesetDb::getLocalState);

            ReplicationManager.update(ohsomePlanetPath, out, replicationElementsUrl, dbUrl, replicationChangesetUrl, false);
            var localStateAfterUpdate = changesetDb.getLocalState();

            assertEquals(6737400, localStateAfterUpdate.getSequenceNumber());
        }
    }
}
