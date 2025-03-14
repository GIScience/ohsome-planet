package org.heigit.ohsome.contributions.util;

public interface Progress {

    void step(long step);

    default void step() {
        step(1);
    }
}
