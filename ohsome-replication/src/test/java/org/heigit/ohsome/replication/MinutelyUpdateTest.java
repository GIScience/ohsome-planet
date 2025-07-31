package org.heigit.ohsome.replication;

import org.junit.jupiter.api.Test;

/**
 * Unit test for simple App.
 */
public class MinutelyUpdateTest {
    @Test
    public void testMinutelyUpdate() {
        var minutelyUpdate = new MinutelyUpdate();
        minutelyUpdate.call();
    }
}
