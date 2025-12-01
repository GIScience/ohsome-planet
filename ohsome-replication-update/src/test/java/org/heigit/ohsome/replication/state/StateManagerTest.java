package org.heigit.ohsome.replication.state;


import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.replication.ReplicationState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;


class StateManagerTest {

    @Container
    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:17-3.5"
            ).asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");


    private static String dbUrl;
    private static final Path RESOURCE_PATH = Path.of("./src/test/resources");

    @BeforeAll
    static void setUp() {
        postgresContainer.withInitScripts("setupChangesetDB.sql", "setupDB/initializeDataForChangesetTests.sql");
        postgresContainer.start();
        dbUrl = postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword();
    }

    @AfterAll
    static void tearDown() {
        postgresContainer.stop();
    }

//    @Test
//    void testStateManagerGetRemoteReplicationState() throws IOException {
//        try (var changesetDb = new ChangesetDB(dbUrl)) {
//           var changesetStateManager = new ChangesetStateManager(changesetDb);
//            var contributionStateManager = new ContributionStateManager(PLANET_OSM_MINUTELY, RESOURCE_PATH, localState, Path.of("."), updateStore, changesetDb);
//
//            var changesetState = changesetStateManager.fetchRemoteState();
//            System.out.println("changesetState = " + changesetState);
//            var contributionState = contributionStateManager.fetchRemoteState();
//            System.out.println("contributionState = " + contributionState);
//
//            assertNotNull(changesetState.getSequenceNumber());
//            assertNotNull(contributionState.getSequenceNumber());
//        }
//    }

    @Test
    void testGetLocalState() throws Exception {
        var changesetStateManager = new ChangesetStateManager(new ChangesetDB(dbUrl));
        changesetStateManager.initializeLocalState();
        assertEquals(10020, changesetStateManager.getLocalState().getSequenceNumber());
    }

    @Test
    void testUpdateLocalState() throws Exception {
        var changesetStateManager = new ChangesetStateManager(new ChangesetDB(dbUrl));
        changesetStateManager.initializeLocalState();
        var localstateBefore = changesetStateManager.getLocalState();
        changesetStateManager.updateLocalState(new ReplicationState(Instant.now(), 1431412));

        assertNotEquals(localstateBefore.getTimestamp(), changesetStateManager.getLocalState().getTimestamp());
        assertEquals(1431412, changesetStateManager.getLocalState().getSequenceNumber());
    }


    @Test
    @Disabled
        // todo: uses actual api
    void testUpdateToRemoteState() throws IOException, SQLException, InterruptedException {
        var changesetStateManager = new ChangesetStateManager(new ChangesetDB(dbUrl));
        var remoteState = changesetStateManager.fetchRemoteState();
        changesetStateManager.updateLocalState(new ReplicationState(Instant.EPOCH, remoteState.getSequenceNumber() - 40));

        changesetStateManager.updateToRemoteState();
        assertEquals(changesetStateManager.getLocalState(), remoteState);

        // todo: add similar for contributions
    }

    @Test
    void testUpdatedUnclosedChangesets() {
        var changesetStateManager = new ChangesetStateManager(new ChangesetDB(dbUrl));
        var changesetDB = new ChangesetDB(dbUrl);
        var changesets = changesetDB.getOpenChangesetsOlderThanTwoHours();
        assertFalse(changesets.isEmpty());

        changesetStateManager.updateUnclosedChangesets();
        changesets = changesetDB.getOpenChangesetsOlderThanTwoHours();
        assertTrue(changesets.isEmpty());
    }
}