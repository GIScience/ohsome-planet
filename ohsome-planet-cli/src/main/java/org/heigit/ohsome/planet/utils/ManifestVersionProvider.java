package org.heigit.ohsome.planet.utils;

import picocli.CommandLine;

import static org.heigit.ohsome.planet.utils.VersionProvider.OHSOME_PLANET_VERSION;

public class ManifestVersionProvider implements CommandLine.IVersionProvider {

    public String[] getVersion() {
        return new String[]{OHSOME_PLANET_VERSION};
    }
}