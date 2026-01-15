package org.heigit.ohsome.planet.utils;

import picocli.CommandLine;

import java.util.Properties;

public class ManifestVersionProvider implements CommandLine.IVersionProvider {

    public String[] getVersion() throws Exception {
        var url = CommandLine.class.getClassLoader().getResource("META-INF/MANIFEST.MF");
        if (url == null) {
            return new String[] {"ohsome-planet"};
        }
        try (var stream = url.openStream()){
            var props = new Properties();
            props.load(stream);
            return new String[]{"%s %s (rev: %s)".formatted(
                    props.getProperty("application"),
                    props.getProperty("version"),
                    props.getProperty("buildnumber"))};
        }
    }
}