package org.heigit.ohsome.replication.state;

import org.heigit.ohsome.replication.processor.ContributionsProcessor;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

class ContributionStateManagerTest {

    private static final Path RESOURCES = Path.of("src/test/resources");

    @Test
    void getState() throws Exception {

        var endpoint = RESOURCES.resolve("replication/minute").toUri().toURL().toString();
        System.out.println("endpoint = " + endpoint);
        Path directory = RESOURCES.resolve("ohsome-planet");
        var out = RESOURCES.resolve("ohsome-planet/out");
        var manager = ContributionStateManager.openManager(endpoint, directory, out);

        var remoteState = manager.fetchRemoteState();
        System.out.println("remoteState = " + remoteState);
        var localState = manager.getLocalState();
        System.out.println("localState = " + localState);

        var processor = processor();
        manager.updateTowardsRemoteState(processor);

    }

    private ContributionsProcessor processor() {
        return null;
    }

}