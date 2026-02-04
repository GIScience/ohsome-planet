package org.heigit.ohsome.planet.utils;

import java.io.IOException;
import java.util.Properties;

public class VersionProvider {

    public static final String OHSOME_PLANET_VERSION = version();

    public static String version()  {
        var url = VersionProvider.class.getClassLoader().getResource("META-INF/MANIFEST.MF");
        if (url == null || !url.toString().contains("ohsome-planet")) {
            return "ohsome-planet";
        }
        try {
            try (var stream = url.openStream()) {
                var props = new Properties();
                props.load(stream);
                var buildnumber = props.getProperty("buildnumber", "-").strip();
                return "%s %s%s".formatted(
                        props.getProperty("application"),
                        props.getProperty("version"),
                        "-".equals(buildnumber) ? "" : " (rev: %s)".formatted(buildnumber));
            }
        } catch (IOException e) {
            return "ohsome-planet";
        }
    }
}
