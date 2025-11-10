package org.heigit.ohsome.replication.state;


import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.heigit.ohsome.replication.state.ContributionStateManager.PLANET_OSM_MINUTELY;
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
        postgresContainer.withInitScript("setupDB/setupChangesetDB.sql");
        postgresContainer.start();
        dbUrl = postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword();
    }

    @AfterAll
    static void tearDown() {
        postgresContainer.stop();
    }

    @Test
    void testStateManagerGetRemoteReplicationState() throws IOException {
        try (var changesetDb = new ChangesetDB(dbUrl)) {
           var changesetStateManager = new ChangesetStateManager(changesetDb);
            var contributionStateManager = new ContributionStateManager(PLANET_OSM_MINUTELY, RESOURCE_PATH, Path.of("."), changesetDb);

            var changesetState = changesetStateManager.fetchRemoteState();
            System.out.println("changesetState = " + changesetState);
            var contributionState = contributionStateManager.fetchRemoteState();
            System.out.println("contributionState = " + contributionState);

            assertNotNull(changesetState.getSequenceNumber());
            assertNotNull(contributionState.getSequenceNumber());
        }
    }

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
    void testUpdateToRemoteState() throws IOException, SQLException {
        var changesetStateManager = new ChangesetStateManager(new ChangesetDB(dbUrl));
        var remoteState = changesetStateManager.fetchRemoteState();
        changesetStateManager.updateLocalState(new ReplicationState(Instant.EPOCH, remoteState.getSequenceNumber() - 40));

        changesetStateManager.updateTowardsRemoteState();
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

    @Test
    void testGettingOldSequenceNumberFromOldTimestamp() throws IOException {
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        var instant = OffsetDateTime.parse("2025-08-14 11:51:33.163329000 +00:00", formatter).toInstant();

        var changesetManager = new ChangesetStateManager(new ChangesetDB(dbUrl));

        for (var time : List.of(
                "2025-08-04T00:00:00Z",
                "2025-08-04T00:10:12Z")) {
            var maxChangesetDbTimetsamp = Instant.parse(time);
            var replication = new ReplicationState(instant, 6642804);
            var oldReplication = changesetManager.estimateLocalReplicationState(
                    maxChangesetDbTimetsamp,
                    replication
            );
            System.out.println("oldReplication = " + oldReplication);
            assertTrue(maxChangesetDbTimetsamp.isBefore(oldReplication.getTimestamp()));
            var secondsBetween = Duration.between(maxChangesetDbTimetsamp, oldReplication.getTimestamp()).toSeconds();
            assertTrue(secondsBetween < 80);
        }
    }
}