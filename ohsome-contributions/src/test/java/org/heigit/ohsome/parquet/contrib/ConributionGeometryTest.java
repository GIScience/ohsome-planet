package org.heigit.ohsome.parquet.contrib;

import static org.junit.jupiter.api.Assertions.*;

import org.heigit.ohsome.contributions.contrib.ContributionGeometry;
import org.junit.jupiter.api.Test;

class ConributionGeometryTest {

  @Test
  void testPolygonFeatureLoading() {
    assertTrue(ContributionGeometry.testPolygonFeature("building", "yes"));
    assertTrue(ContributionGeometry.testPolygonFeature("natural", "meadow"));
    assertTrue(ContributionGeometry.testPolygonFeature("waterway", "dock"));

    assertFalse(ContributionGeometry.testPolygonFeature("surface","asphalt"));
    assertFalse(ContributionGeometry.testPolygonFeature("highway","primary"));
    assertFalse(ContributionGeometry.testPolygonFeature("natural","coastline"));

  }

}