# Frequently Asked Questions

## Why is the geometry of some OSM relations just a bounding box?

For relations that consist of more than 500 members, we skip `MultiPolygon` geometry building and fall back to `GeometryCollection`. This [threshold can be changed](#can-i-configure-the-way-geometries-are-build-for-relations-with-many-members?)


## Can I configure the way geometries are build for relations with many members?

For relations that consist of more than 500 members we skip `MultiPolygon` geometry building and fall back to `GeometryCollection`.
To configure this threshold (e.g. 500 members) check the `MEMBERS_THRESHOLD` variable in `ohsome-contributions/src/main/java/org/heigit/ohsome/contributions/contrib/ContributionGeometry.java`.


## Are there any examples of how to query the ohsome-planet dataset with DuckDB?

Yes, there are! Please [have a look at those](useful_queries.md).


## How can I filter for deleted OSM elements?

The raw OSM data does not provide any geometry for deleted elements, but ohsome-planet does.
For contributions with status `deleted` we use the geometry of the previous version.
This allows you to spatially filter also for deleted elements, e.g. by bounding box.


