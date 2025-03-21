package org.heigit.ohsome.contributions.util;

import java.util.HashMap;
import java.util.Map;

/**
 * based on the following paper:
 * Böhm, C., Klump, G., Kriegel, HP. (1999). XZ-Ordering: A Space-Filling Curve for Objects with Spatial Extension. In: Güting, R.H., Papadias, D., Lochovsky, F. (eds) Advances in Spatial Databases. SSD 1999. Lecture Notes in Computer Science, vol 1651. Springer, Berlin, Heidelberg. https://doi.org/10.1007/3-540-48482-5_7
 *
 * https://www.dbs.ifi.lmu.de/Publikationen/Boehm/Ordering_99.pdf
 *
 * https://github.com/locationtech/geowave/blob/master/core/index/src/main/java/org/locationtech/geowave/core/index/sfc/xz/XZOrderSFC.java
 */
public class XZCode {
    public record XZLevelCode(int level, long code) {}

    private static final Map<Integer, XZCode> INSTANCES = new HashMap<>();
    private static final double LOG_POINT_FIVE = Math.log(0.5);

    private final int g;
    private final long[] nElem;

    public XZCode(int g) {
        this.g = g;
        this.nElem = new long[g];
        for (var i=0; i < g; i++) {
            nElem[i] = (((long) Math.pow(4, g - i)) - 1L) / 3L;
        }
    }

    public static XZLevelCode getId(int g, double xmin, double ymin, double xmax, double ymax) {
        return INSTANCES.computeIfAbsent(g, XZCode::new).getId(xmin, ymin, xmax, ymax);
    }

    public XZLevelCode getId(double xmin, double ymin, double xmax, double ymax) {
        xmin = (180.0 + xmin) / 360.0;
        ymin = (90.0 + ymin) / 360.0;
        xmax = (180.0 + xmax) / 360.0;
        ymax = (90.0 + ymax) / 360.0;

        final int l1 = (int) Math.floor(Math.log(Math.max(xmax - xmin, ymax - ymin)) / LOG_POINT_FIVE);
        // the length will either be (l1) or (l1 + 1)
        int length = g;
        if (l1 < g) {
            length = l1 + 1;
            final double w2 = Math.pow(0.5, length); // width of an element at
            // resolution l2 (l1 + 1)
            if (!predicate(xmin, xmax, w2) || !predicate(ymin, ymax, w2)) {
                length = l1;
            }
        }
        return sequenceCode(xmin, ymin, length);
    }

    private XZLevelCode sequenceCode(double x, double y, int length) {
        var xmin = 0.0;
        var xmax = 1.0;
        var ymin = 0.0;
        var ymax = 1.0;

        var cs = 0L;
        for (var i = 0; i< length; i++) {
            var q = 0L;
            var xc = xmin + ((xmax - xmin) / 2);
            if ( x >= xc) {
                q += 1;
                xmin = xc;
            } else {
                xmax = xc;
            }
            var yc = ymin + ((ymax - ymin) / 2);
            if ( y >= yc) {
                q += 2;
                ymin = yc;
            } else {
                ymax = yc;
            }

            cs += q * nElem[i] + 1;
        }

        return new XZLevelCode(length, cs);
    }

    // predicate for checking how many axis the polygon intersects
    // math.floor(min / w2) * w2 == start of cell containing min
    private static boolean predicate(final double min, final double max, final double w) {
        return max <= ((Math.floor(min / w) * w) + (2 * w));
    }
}