package org.heigit.ohsome.replication.state;

import com.google.common.io.MoreFiles;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ContributionStateManagerTest {

    private static final Path RESOURCES = Path.of("src/test/resources");

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
    void getState() throws Exception {
        var endpointPath = RESOURCES.resolve("replication/minute");
        var endpointUrl = endpointPath.toUri().toURL().toString();

        var rootDir = Path.of("test-output/ohsome-planet");
        if (Files.exists(rootDir)) {
            MoreFiles.deleteRecursively(rootDir);
        }
        var out = rootDir.resolve("out");
        try(var changesetDb = new ChangesetDB(dbUrl)) {
            var manager = ContributionStateManager.openManager(endpointUrl, rootDir, out, changesetDb);
            var remoteState = manager.fetchRemoteState();
            System.out.println("remoteState = " + remoteState);
            var localState = ReplicationState.read(endpointPath.resolve("006/824/839.state.txt"));
            manager.updateLocalState(localState);
            System.out.println("localState = " + localState);
            manager.updateTowardsRemoteState();

            var changesets = changesetDb.changesets(Set.of(1L), this::changeset);
            System.out.println("changesets = " + changesets);

        } finally {
//            MoreFiles.deleteRecursively(rootDir);
        }

    }

    private Long changeset(long id, Instant created, Instant closed, Map<String, String> tags, List<String> hashtags, String editor) {
        return id;
    }

}