package org.heigit.ohsome.replication;

import static org.junit.jupiter.api.Assertions.*;

class ServerTest {
//
//    @Test
//    void testGettingOldSequenceNumberFromOldTimestamp() throws IOException {
//        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
//        var instant = OffsetDateTime.parse("2025-08-14 11:51:33.163329000 +00:00", formatter).toInstant();
//
//        var changesetManager = new ChangesetStateManager(new ChangesetDB(dbUrl));
//        instant = changesetManager.fetchRemoteState().getTimestamp();
//
//        for (var time : List.of(
//                "2025-08-04T00:00:00Z",
//                "2025-08-04T00:10:12Z")) {
//            var maxChangesetDbTimestamp = Instant.parse(time);
//            var replication = new ReplicationState(instant, 6642804);
//            var oldReplication = changesetManager.estimateLocalReplicationState(
//                    maxChangesetDbTimestamp,
//                    replication
//            );
//            System.out.println("oldReplication = " + oldReplication);
//            assertTrue(maxChangesetDbTimestamp.isBefore(oldReplication.getTimestamp()));
//            var secondsBetween = Duration.between(maxChangesetDbTimestamp, oldReplication.getTimestamp()).toSeconds();
//            assertTrue(secondsBetween < 80);
//        }
//    }
//
//    @Test
//    void testGettingOldSequenceNumberFromOldTimestampContributions() throws IOException {
//        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
//        var instant = OffsetDateTime.parse("2025-08-14 11:51:33.163329000 +00:00", formatter).toInstant();
//
//        var path = Files.createTempDirectory("test-updatestore");
//
//        var contributionStateManager = ContributionStateManager.openManager("https://planet.openstreetmap.org/replication/minute", path, path, UpdateStore.NOOP, IChangesetDB.NOOP);
//        instant = contributionStateManager.fetchRemoteState().getTimestamp();
//
//        for (var time : List.of(
//                "2025-08-02T00:00:00Z",
//                "2025-08-04T00:10:12Z")) {
//            var maxChangesetDbTimestamp = Instant.parse(time);
//            var replication = new ReplicationState(instant, 6642804);
//            var oldReplication = contributionStateManager.estimateLocalReplicationState(
//                    maxChangesetDbTimestamp,
//                    replication
//            );
//            System.out.println("oldReplication = " + oldReplication);
//            assertTrue(maxChangesetDbTimestamp.isBefore(oldReplication.getTimestamp()));
//            var secondsBetween = Duration.between(maxChangesetDbTimestamp, oldReplication.getTimestamp()).toSeconds();
//            assertTrue(secondsBetween < 80);
//        }
//    }
//
//
//    @Test
//    void testGettingOldSequenceNumberFromOldTimestampWithOtherMethod() throws IOException, URISyntaxException {
//        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
//        var instant = OffsetDateTime.parse("2025-08-14 11:51:33.163329000 +00:00", formatter).toInstant();
//
//
//        for (var time : List.of(
//                "2025-08-02T00:00:00Z",
//                "2025-08-04T00:10:12Z")) {
//            var maxChangesetDbTimestamp = Instant.parse(time);
//            var oldReplication = tryToFindStartFromTimestamp(
//                    new URI("https://planet.openstreetmap.org/replication/minute").toURL(),
//                    maxChangesetDbTimestamp
//            );
//            System.out.println("oldReplication = " + oldReplication);
//            //assertTrue(maxChangesetDbTimestamp.isBefore(oldReplication.getTimestamp()));
//            //var secondsBetween = Duration.between(maxChangesetDbTimestamp, oldReplication.getTimestamp()).toSeconds();
//            //assertTrue(secondsBetween < 80);
//        }
//    }
}