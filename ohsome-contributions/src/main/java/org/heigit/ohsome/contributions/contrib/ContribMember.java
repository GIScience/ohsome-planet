package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMType;

public record ContribMember(OSMType type, long id, Contribution contrib, String role) {
}
