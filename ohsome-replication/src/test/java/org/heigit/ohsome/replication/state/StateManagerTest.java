package org.heigit.ohsome.replication.state;


import org.heigit.ohsome.replication.databases.KeyValueDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;
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
    private static final String RESOURCE_PATH = "./src/test/resources";

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
    void testStateManagerGetRemoteReplicationState() {
        var changesetStateManager = new ChangesetStateManager(dbUrl);
        var contributionStateManager = new ContributionStateManager("minute", new KeyValueDB(Path.of(RESOURCE_PATH)));

        var changesetState = changesetStateManager.fetchRemoteState();
        System.out.println("changesetState = " + changesetState);
        var contributionState = contributionStateManager.fetchRemoteState();
        System.out.println("contributionState = " + contributionState);

        assertNotNull(changesetState.getSequenceNumber());
        assertNotNull(contributionState.getSequenceNumber());
    }

    @Test
    void testGetLocalState() {
        var changesetStateManager = new ChangesetStateManager(dbUrl);
        changesetStateManager.initializeLocalState();
        assertEquals(10020, changesetStateManager.getLocalState().getSequenceNumber());
    }

    @Test
    void testUpdateLocalState() {
        var changesetStateManager = new ChangesetStateManager(dbUrl);
        changesetStateManager.initializeLocalState();
        var localstateBefore = changesetStateManager.getLocalState();
        changesetStateManager.updateLocalState(new ReplicationState(Instant.now(), 1431412));

        assertNotEquals(localstateBefore.getTimestamp(), changesetStateManager.getLocalState().getTimestamp());
        assertEquals(1431412, changesetStateManager.getLocalState().getSequenceNumber());
    }


    @Test
    void testUpdateToRemoteState() {
        var changesetStateManager = new ChangesetStateManager(dbUrl);
        var remoteState = changesetStateManager.fetchRemoteState();
        changesetStateManager.updateLocalState(new ReplicationState(Instant.EPOCH, remoteState.getSequenceNumber() - 1));

        changesetStateManager.updateTowardsRemoteState();
        assertEquals(changesetStateManager.getLocalState(), remoteState);

        // todo: add similar for contributions
    }

    @Test
    void testUpdatedUnclosedChangesets() {
        var changesetStateManager = new ChangesetStateManager(dbUrl);
        var nowClosed = changesetStateManager.updateUnclosedChangesets();
        assertArrayEquals(new long[]{34123412, 1231, 111}, nowClosed.stream().mapToLong(OSMChangeset::id).toArray());
    }

    @Test
    void testGettingOldSequenceNumberFromOldTimestamp() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        var instant = OffsetDateTime.parse("2025-08-14 11:51:33.163329000 +00:00", formatter).toInstant();

        var changesetManager = new ChangesetStateManager(dbUrl);

        var replication = new ReplicationState(instant, 6642804);
        var oldReplication = changesetManager.estimateLocalReplicationState(
                Instant.parse("2025-08-04T00:00:00Z"),
                replication
        );
        System.out.println("oldReplication = " + oldReplication);

    }
}