package org.heigit.ohsome.replication;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerTest {
    @Test
    @Disabled
    void testGettingOldSequenceNumberFromOldTimestamp() throws IOException, InterruptedException {
        var server = Server.OSM_CHANGESET_SERVER;

        for (var time : List.of(
                "2025-08-04T00:00:00Z",
                "2025-08-04T00:10:12Z",
                "2025-11-03T00:59:59Z")) {
            var maxChangesetDbTimestamp = Instant.parse(time);
            var replication = server.getLatestRemoteState();
            var oldReplication = server.findStartStateByTimestamp(
                    maxChangesetDbTimestamp,
                    replication
            );
            System.out.println("oldReplication = " + oldReplication);
            System.out.println("timestamp = " + time);
            assertTrue(maxChangesetDbTimestamp.isAfter(oldReplication.getTimestamp()));
            var secondsBetween = Duration.between(maxChangesetDbTimestamp, oldReplication.getTimestamp()).toSeconds();
            assertTrue(secondsBetween < 120);
        }
    }

    @Test
    @Disabled
    void testGettingOldSequenceNumberFromOldTimestampContributions() throws IOException, InterruptedException, URISyntaxException {
        var server = Server.osmEntityServer("https://planet.openstreetmap.org/replication/minute/");

        for (var time : List.of(
                "2025-08-02T00:00:00Z",
                "2025-08-04T00:10:12Z")) {
            var maxChangesetDbTimestamp = Instant.parse(time);
            var replication = new ReplicationState(Instant.parse("2025-11-20T20:41:49Z"), 6866347);
            var oldReplication = server.findStartStateByTimestamp(
                    maxChangesetDbTimestamp,
                    replication
            );
            System.out.println("oldReplication = " + oldReplication);
            System.out.println("timestamp = " + time);

            assertTrue(maxChangesetDbTimestamp.isAfter(oldReplication.getTimestamp()));
            var secondsBetween = Duration.between(maxChangesetDbTimestamp, oldReplication.getTimestamp()).toSeconds();
            assertTrue(secondsBetween < 120);
        }
    }
}