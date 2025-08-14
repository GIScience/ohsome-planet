package org.heigit.ohsome.replication.state;


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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class StateManagerTest {

    @Container
    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:17-3.5"
            ).asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");


    private static String db_url;

    @BeforeAll
    public static void setUp() {
        postgresContainer.withInitScript("setupDB/setupChangesetDB.sql");
        postgresContainer.start();
        db_url = postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword();
    }

    @AfterAll
    public static void tearDown() {
        postgresContainer.stop();
    }

    @Test
    public void testStateManagerGetRemoteReplicationState() {
        var changesetStateManager = new ChangesetStateManager(db_url);
        var contributionStateManager = new ContributionStateManager("minute", Path.of(""));

        var changesetState = changesetStateManager.getRemoteState();
        System.out.println("changesetState = " + changesetState);
        var contributionState = contributionStateManager.getRemoteState();
        System.out.println("contributionState = " + contributionState);
    }

    @Test
    public void testStateManagerGetRemoteReplicationData() {
        var changesetStateManager = new ChangesetStateManager(db_url);
        var contributionStateManager = new ContributionStateManager("minute", Path.of(""));

        var changesetReplicationFile = changesetStateManager.getReplicationFile("000/021/212");
        System.out.println("changesetReplicationFile = " + changesetReplicationFile);
        var contributionReplicationFile = contributionStateManager.getReplicationFile("000/021/212");
        System.out.println("contributionReplicationFile = " + contributionReplicationFile);
    }

    @Test
    public void testGetLocalState() {
        var changesetStateManager = new ChangesetStateManager(db_url);
        changesetStateManager.initializeLocalState();
        assertEquals(10020, changesetStateManager.localState.sequenceNumber);
    }

    @Test
    public void testUpdateLocalState() {
        var changesetStateManager = new ChangesetStateManager(db_url);
        changesetStateManager.initializeLocalState();
        var localstateBefore = changesetStateManager.localState;
        changesetStateManager.updateLocalState(new ReplicationState(Instant.now(), 1431412));

        assertNotEquals(localstateBefore.timestamp, changesetStateManager.localState.timestamp);
        assertEquals(1431412, changesetStateManager.localState.sequenceNumber);
    }


    @Test
    public void testFetchReplicationBatch() {
        var changesetStateManager = new ChangesetStateManager(db_url);
        var batch = changesetStateManager.fetchReplicationBatch("006/021/212");
        for (var batchElement : batch) {
            System.out.println(batchElement);
        }

        var contributionStateManager = new ContributionStateManager("minute", Path.of(""));
        var contributionBatch = contributionStateManager.fetchReplicationBatch("000/021/212");
        for (var batchElement : contributionBatch) {
            System.out.println(batchElement);
        }

    }

    @Test
    public void testUpdateToRemoteState() {
        var changesetStateManager = new ChangesetStateManager(db_url);
        var remoteState = changesetStateManager.getRemoteState();
        changesetStateManager.updateLocalState(new ReplicationState(Instant.EPOCH, remoteState.sequenceNumber - 5));

        changesetStateManager.updateTowardsRemoteState();
    }

    @Test
    public void testUpdatedUnclosedChangesets() {
        var changesetStateManager = new ChangesetStateManager(db_url);
        var now_closed = changesetStateManager.updateUnclosedChangesets();
        for (var batchElement : now_closed) {
            System.out.println(batchElement);
        }
    }

    @Test
    public void testGettingOldSequenceNumberFromOldTimestamp() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        var instant = OffsetDateTime.parse("2025-08-14 11:51:33.163329000 +00:00", formatter).toInstant();

        var changesetManager = new ChangesetStateManager(db_url);

        var replication = new ReplicationState(instant, 6642804);
        var oldReplication = changesetManager.oldSequenceNumberFromDifferenceToOldTimestamp(
                Instant.parse("2025-08-04T00:00:00Z"),
                changesetManager.getRemoteState()
        );
        System.out.println("oldReplication = " + oldReplication);

    }
}