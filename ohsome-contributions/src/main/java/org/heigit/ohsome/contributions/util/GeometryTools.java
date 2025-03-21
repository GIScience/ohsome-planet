package org.heigit.ohsome.contributions.util;

import org.locationtech.jts.geom.*;

public class GeometryTools {

    private static final double earthRadiusMean = 6371000.0; //meters
    private static final double earthRadiusEquator = 6378137.0; //meters
    private static final double earthInverseFlattening = 298.257223563;
    private static final double f_ = 1.0 - 1.0 / earthInverseFlattening;
    // this partially accounts for the non-spherical shape of the earth
    // see https://gis.stackexchange.com/a/63047/41632
    private static final double sphereFact = Math.pow(f_, 1.5);

    /**
     * Calculate the approximate length of an arbitrary geometry.
     *
     * <p>Returns zero for non-linear features such as points or polygons.</p>
     *
     * <p>See {@link #lengthOf(LineString)} for further details.</p>
     *
     * @param geom the geometry to get the length of. coordinates must be in WGS84
     * @return The approximate geodesic length of the geometry in meters.
     */
    public static double lengthOf(Geometry geom) {
        return switch (geom) {
            case LineString line -> lengthOf(line);
            case MultiLineString multiLine -> lengthOf(multiLine);
//            case GeometryCollection geometryCollection -> lengthOf(geometryCollection);
            default -> 0.0;
        };
    }

    public static double lengthOf(LineString line) {
        return lengthOf(line.getCoordinates());
    }

    private static double lengthOf(Coordinate[] coords) {
        double dist = 0.0;
        if (coords.length > 1) {
            double prevLon = Math.toRadians(coords[0].x);
            double prevLat = Math.atan(sphereFact * Math.tan(Math.toRadians(coords[0].y)));
            for (int i = 1; i < coords.length; i++) {
                double thisLon = Math.toRadians(coords[i].x);
                double thisLat = Math.atan(sphereFact * Math.tan(Math.toRadians(coords[i].y)));
                double deltaLon = thisLon - prevLon;
                double deltaLat = thisLat - prevLat;
                deltaLon *= Math.cos((thisLat + prevLat) / 2);
                dist += Math.sqrt(deltaLon * deltaLon + deltaLat * deltaLat);
                prevLon = thisLon;
                prevLat = thisLat;
            }
            dist *= earthRadiusMean;
        }
        return dist;
    }

    /**
     * Calculate the approximate length of a multi geometry.
     *
     * <p>See {@link #lengthOf(LineString)} for further details.</p>
     *
     * @param geometryCollection the geometries to get the length of. coordinates must be in WGS84
     * @return The approximate geodesic length of the linear geometries of this collection in meters.
     */
    public static double lengthOf(GeometryCollection geometryCollection) {
        double dist = 0.0;
        for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
            dist += lengthOf(geometryCollection.getGeometryN(i));
        }
        return dist;
    }

    /**
     * Calculate the approximate area of an arbitrary geometry.
     *
     * <p>Returns zero for non-polygonal features such as points or lines.</p>
     *
     * <p>See {@link #areaOf(Polygon)} for further details.</p>
     *
     * @param geom the geometry for which the area should be calculated. coordinates must be in WGS84
     * @return The approximate signed geodesic area of the geometry in square meters.
     */
    public static double areaOf(Geometry geom) {
        return switch (geom) {
            case Polygon polygon -> areaOf(polygon);
            case MultiPolygon multiPolygon -> areaOf(multiPolygon);
//            case GeometryCollection geometryCollection -> areaOf(geometryCollection);
            default -> 0.0;
        };
    }


    /**
     * Calculate the approximate area of an arbitrary geometry.
     *
     * <p>
     * This uses approximation formulas for the area of small polygons on a sphere, but partially
     * accounts for the spheroidal shape of the earth (geometry coordinates given in WGS84).
     * </p>
     *
     * <p>
     * For typical features present in OpenStreetMap data, the relative error introduced by
     * this approximation is below 0.1%
     * </p>
     *
     * <p>
     * Reference:
     * Robert. G. Chamberlain and William H. Duquette, "Some Algorithms for
     * Polygons on a Sphere", JPL Publication 07-03, Jet Propulsion
     * Laboratory, Pasadena, CA, June 2007
     * https://trs.jpl.nasa.gov/handle/2014/40409
     * </p>
     *
     * <p>
     * This method will never return a negative number. For invalid polygons with a larger inner
     * rings area than the outer ring encompasses, zero is returned instead.
     * </p>
     *
     * @param poly the polygon for which the area should be calculated. coordinates must be in WGS84
     * @return The approximate signed geodesic area of the polygon in square meters.
     */
    public static double areaOf(Polygon poly) {
        double area = 0.0;
        area += Math.abs(ringArea(poly.getExteriorRing()));
        for (int i = 0; i < poly.getNumInteriorRing(); i++) {
            area -= Math.abs(ringArea(poly.getInteriorRingN(i)));
        }
        return Math.max(0, area);
    }

    /**
     * Calculate the approximate area of a multi polygon.
     *
     * <p>See {@link #areaOf(Polygon)} for further details.</p>
     *
     * @param multipoly the multipolygon for which the area should be calculated.
     *                  coordinates must be in WGS84
     * @return The approximate signed geodesic area of the multipolygon in square meters.
     */
    public static double areaOf(MultiPolygon multipoly) {
        double area = 0.0;
        for (int i = 0; i < multipoly.getNumGeometries(); i++) {
            area += areaOf((Polygon) multipoly.getGeometryN(i));
        }
        return area;
    }


    /**
     * Calculate the approximate area of a geometry collection.
     *
     * <p>See {@link #areaOf(Polygon)} for further details.</p>
     *
     * @param geometryCollection the geometry collection for which the area should be calculated.
     *                           coordinates must be in WGS84
     * @return The approximate signed geodesic area of the geometry collection in square meters.
     */
    public static double areaOf(GeometryCollection geometryCollection) {
        double area = 0.0;
        for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
            area += areaOf(geometryCollection.getGeometryN(i));
        }
        return area;
    }


    /**
     * Calculate the approximate area of the polygon.
     *
     * <p>
     * Note that this area will be positive if ring is oriented clockwise,
     * otherwise it will be negative.
     * </p>
     *
     * <p>
     * Initially ported to Java from https://github.com/mapbox/geojson-area/.
     * Later adjusted to partially account for the spheroidal shape of the earth (WGS84 coordinates).
     * </p>
     *
     * <p>
     * For typical features present in OpenStreetMap data, the relative error introduced by
     * this approximation is below 0.1%
     * </p>
     *
     * <p>
     * Reference:
     * Robert. G. Chamberlain and William H. Duquette, "Some Algorithms for
     * Polygons on a Sphere", JPL Publication 07-03, Jet Propulsion
     * Laboratory, Pasadena, CA, June 2007
     * https://trs.jpl.nasa.gov/handle/2014/40409
     * </p>
     *
     * @param ring the closed ring delimiting the area to be calculated. coordinates must be in WGS84
     * @return The approximate signed geodesic area of the polygon in square meters.
     */
    private static double ringArea(LinearRing ring) {
        double area = 0.0;
        Coordinate[] coords = ring.getCoordinates();
        int coordsLength = coords.length;

        if (coordsLength > 2) {
            for (int i = 0; i < coordsLength; i++) {
                int lowerIndex;
                int middleIndex;
                int upperIndex;
                if (i == coordsLength - 2) { // i = N-2
                    lowerIndex = coordsLength - 2;
                    middleIndex = coordsLength - 1;
                    upperIndex = 0;
                } else if (i == coordsLength - 1) { // i = N-1
                    lowerIndex = coordsLength - 1;
                    middleIndex = 0;
                    upperIndex = 1;
                } else { // i = 0 to N-3
                    lowerIndex = i;
                    middleIndex = i + 1;
                    upperIndex = i + 2;
                }
                Coordinate p1 = coords[lowerIndex];
                Coordinate p2 = coords[middleIndex];
                Coordinate p3 = coords[upperIndex];
                // wgs84 latitudes are not the same as spherical latitudes.
                // this converts the latitude from a wgs84 coordinate into its corresponding spherical value
                double x = f_ * Math.tan(Math.toRadians(p2.y));
                double sinLat = x / Math.sqrt(x * x + 1.0);
                area += (Math.toRadians(p3.x - p1.x)) * sinLat;
            }
            double midLat = (ring.getEnvelopeInternal().getMaxY() + ring.getEnvelopeInternal().getMinY()) / 2;
            area *= 0.5 * earthRadiusEquator * earthRadiusEquator * (1 - 1 / earthInverseFlattening * Math.pow(Math.cos(Math.toRadians(midLat)), 2));
        }

        return area;
    }
}
