package org.heigit.ohsome.replication;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;

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
        var ohsomePlanetPath = RESOURCE_PATH.resolve("ohsome-planet");
        System.out.println("replicationChangesetUrl = " + replicationChangesetUrl);

//        var changesetManager = new ChangesetStateManager(replicationChangesetUrl, dbUrl);
//        changesetManager.initializeLocalState();

//        ReplicationManager.update(ohsomePlanetPath, dbUrl, replicationChangesetUrl);
    }
}
