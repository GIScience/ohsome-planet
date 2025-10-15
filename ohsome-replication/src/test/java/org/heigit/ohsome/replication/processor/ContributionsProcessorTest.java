package org.heigit.ohsome.replication.processor;

import org.heigit.ohsome.replication.update.ContributionUpdaterTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ContributionsProcessorTest {

    @Test
    void process() throws IOException {
        var process = new ContributionsProcessor();

        process.update(List.of(
                ContributionUpdaterTest.node(1, 1, 1, 1),
                ContributionUpdaterTest.node(2, 1, 1, 1),
                ContributionUpdaterTest.way(1, 1, 1, 1, List.of(1L, 2L))
        ), 1);

    }

}