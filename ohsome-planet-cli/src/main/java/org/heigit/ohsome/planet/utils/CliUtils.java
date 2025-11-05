package org.heigit.ohsome.planet.utils;

public class CliUtils {
    private CliUtils() {
        // utility class
    }

    public static void setVerbosity(boolean[] verbosity){
        if (verbosity != null) {
            var levels = new String[]{"warn", "info", "debug", "trace"};
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", levels[verbosity.length < levels.length ? verbosity.length : levels.length - 1]);
        }
    }
}
