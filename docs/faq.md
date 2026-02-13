# Frequently Asked Questions

## Why is the geometry of some OSM relations just a bounding box?

For relations that consist of more than 500 members, we skip `MultiPolygon` geometry building and fall back to `GeometryCollection`. This [threshold can be changed](#can-i-configure-the-way-geometries-are-build-for-relations-with-many-members?)


## Can I configure the way geometries are build for relations with many members?

For relations that consist of more than 500 members we skip `MultiPolygon` geometry building and fall back to `GeometryCollection`.
To configure this threshold (e.g. 500 members) check the `MEMBERS_THRESHOLD` variable in `ohsome-contributions/src/main/java/org/heigit/ohsome/contributions/contrib/ContributionGeometry.java`.


## Are there any examples of how to query the ohsome-planet dataset with DuckDB?

Yes, there are! Please [have a look at those](docs/usefule_queries).
