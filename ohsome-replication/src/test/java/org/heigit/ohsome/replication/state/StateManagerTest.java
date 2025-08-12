package org.heigit.ohsome.replication.state;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;

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

    @BeforeAll
    public static void setUp() {
        postgresContainer.withInitScript("setupDB/setupChangesetDB.sql");
        postgresContainer.start();
        System.setProperty("DB_URL", postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword());
    }

    @AfterAll
    public static void tearDown() {
        postgresContainer.stop();
    }

    @Test
    public void testStateManagerGetRemoteReplicationState() {
        var changesetStateManager = new ChangesetStateManager();
        var contributionStateManager = new ContributionStateManager("minute");

        var changesetState = changesetStateManager.getRemoteState();
        System.out.println("changesetState = " + changesetState);
        var contributionState = contributionStateManager.getRemoteState();
        System.out.println("contributionState = " + contributionState);
    }

    @Test
    public void testStateManagerGetRemoteReplicationData() {
        var changesetStateManager = new ChangesetStateManager();
        var contributionStateManager = new ContributionStateManager("minute");

        var changesetReplicationFile = changesetStateManager.getReplicationFile("000/021/212");
        System.out.println("changesetReplicationFile = " + changesetReplicationFile);
        var contributionReplicationFile = contributionStateManager.getReplicationFile("000/021/212");
        System.out.println("contributionReplicationFile = " + contributionReplicationFile);
    }

    @Test
    public void testGetLocalState() {
        var changesetStateManager = new ChangesetStateManager();
        var localstate = changesetStateManager.getLocalState();
        assertEquals(10020, localstate.sequenceNumber);
    }

    @Test
    public void testUpdateLocalState() {
        var changesetStateManager = new ChangesetStateManager();
        var localstateBefore = changesetStateManager.getLocalState();

        changesetStateManager.updateLocalState(new ReplicationState(Instant.now(), 1431412));
        var localstateAfter = changesetStateManager.getLocalState();

        assertNotEquals(localstateBefore.timestamp, localstateAfter.timestamp);
        assertEquals(1431412, localstateAfter.sequenceNumber);
    }


    @Test
    public void testFetchReplicationBatch() {
        var changesetStateManager = new ChangesetStateManager();
        var batch = changesetStateManager.fetchReplicationBatch("006/021/212");
        for (var batchElement : batch) {
            System.out.println(batchElement);
        }

        var contributionStateManager = new ContributionStateManager("minute");
        var contributionBatch = contributionStateManager.fetchReplicationBatch("000/021/212");
        for (var batchElement : contributionBatch) {
            System.out.println(batchElement);
        }

    }

    @Test
    public void testUpdateToRemoteState() {
        var changesetStateManager = new ChangesetStateManager();
        var remoteState = changesetStateManager.getRemoteState();
        changesetStateManager.updateLocalState(new ReplicationState(Instant.EPOCH, remoteState.sequenceNumber - 5));

        changesetStateManager.updateToRemoteState();
    }

    @Test
    public void testUpdatedUnclosedChangesets() {
        var changesetStateManager = new ChangesetStateManager();
        var now_closed = changesetStateManager.updateUnclosedChangesets();
        for (var batchElement : now_closed) {
            System.out.println(batchElement);
        }
    }
}