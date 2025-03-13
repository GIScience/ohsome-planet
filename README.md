# ohsome-planet

The ohsome-planet tool transforms OSM (history) PBF files into GeoParquet format.
It creates the actual OSM elements geometries for nodes, ways and relations.
The tool can join information from OSM changesets such as hashtags, OSM editor or usernames.
You can join country codes to every OSM element by passing a boundary dataset as additional input.

You can use the ohsome-planet data to perform a wide range of geospatial analyses, e.g. using DuckDB, GeoPandas or QGIS.
Display the data directly on a map and start playing around!


## Requirements
- java 21

## Build

First, clone the repository and its submodules. Then, build it with Maven.
```shell
git clone --recurse-submodules https://github.com/GIScience/ohsome-planet.git
cd ohsome-planet
./mvnw clean package -DskipTests
```

## Run

You can download the [full latest or history planet](https://planet.openstreetmap.org/pbf/full-history/) 
or download PBF files for smaller regions from [Geofabrik](https://osm-internal.download.geofabrik.de/).

To process a given PBF file, provide it in the `--pbf` parameter in the following example.
```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --pbf data/karlsruhe.osh.pbf \
    --country-file data/world.csv \
    --output out-karlsruhe \
    --overwrite 
```
The parameters `--country-file`, `--output` and `--overwrite` are optional. To see all available parameters, call the tool with `--help` parameter.

## Output Structure


```
out-karlsruhe
    contributions/latest
    minorNodes/
    minorWays/
    node-000.parquet
    node-001.parquet
    ...
    relation-000.parquet
    relation-001.parquet
    ...
    way-000.parquet
    way-001.parquet
    ...
```

## Inspect Results
You can inspect your results easily using [DuckDB](https://duckdb.org/docs/installation).

```sql
-- list all columns
DESCRIBE FROM read_parquet('out-karlruhe/*.parquet');

-- result
┌───────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
│    column_name    │                                                                column_type                                                                 │  null   │   key   │ default │  extra  │
│      varchar      │                                                                  varchar                                                                   │ varchar │ varchar │ varchar │ varchar │
├───────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼─────────┼─────────┼─────────┼─────────┤
│ status            │ VARCHAR                                                                                                                                    │ YES     │         │         │         │
│ valid_from        │ TIMESTAMP WITH TIME ZONE                                                                                                                   │ YES     │         │         │         │
│ valid_to          │ TIMESTAMP WITH TIME ZONE                                                                                                                   │ YES     │         │         │         │
│ osm_type          │ VARCHAR                                                                                                                                    │ YES     │         │         │         │
│ osm_id            │ BIGINT                                                                                                                                     │ YES     │         │         │         │
│ osm_version       │ INTEGER                                                                                                                                    │ YES     │         │         │         │
│ osm_minor_version │ INTEGER                                                                                                                                    │ YES     │         │         │         │
│ user              │ STRUCT(id INTEGER, "name" VARCHAR)                                                                                                         │ YES     │         │         │         │
│ tags              │ MAP(VARCHAR, VARCHAR)                                                                                                                      │ YES     │         │         │         │
│ tags_added        │ MAP(VARCHAR, VARCHAR)                                                                                                                      │ YES     │         │         │         │
│ tags_removed      │ MAP(VARCHAR, VARCHAR)                                                                                                                      │ YES     │         │         │         │
│ changeset         │ STRUCT(id BIGINT, created_at TIMESTAMP WITH TIME ZONE, closed_at TIMESTAMP WITH TIME ZONE, tags MAP(VARCHAR, VARCHAR), hashtags VARCHAR[]) │ YES     │         │         │         │
│ bbox              │ STRUCT(minX DOUBLE, minY DOUBLE, maxX DOUBLE, maxY DOUBLE)                                                                                 │ YES     │         │         │         │
│ centroid          │ STRUCT(x DOUBLE, y DOUBLE)                                                                                                                 │ YES     │         │         │         │
│ geometry_type     │ VARCHAR                                                                                                                                    │ YES     │         │         │         │
│ geometry          │ BLOB                                                                                                                                       │ YES     │         │         │         │
│ area              │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ area_delta        │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ length            │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ length_delta      │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ contrib_type      │ VARCHAR                                                                                                                                    │ YES     │         │         │         │
│ refs              │ BIGINT[]                                                                                                                                   │ YES     │         │         │         │
│ members           │ STRUCT("type" VARCHAR, id BIGINT, "role" VARCHAR, geometry BLOB)[]                                                                         │ YES     │         │         │         │
├───────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┤
│ 23 rows                                                                                                                                                                                      6 columns │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Getting Started as Developer
This is a list of resources that you might want to take a look at to get a better understanding of the core concepts used for this projects. 
In general, you should gain some understanding of the raw OSM (history) data format and know how to build geometries from nodes, ways and relations.
Furthermore, knowledge about (Geo)Parquet files is useful as well.

What is the OSM PBF File Format?
* https://wiki.openstreetmap.org/wiki/PBF_Format
* you can download history PBF files for smaller regions from [Geofabrik](https://osm-internal.download.geofabrik.de/)
* full planet downloads: https://planet.openstreetmap.org/planet/full-history/

What is parquet?
* https://parquet.apache.org/docs/file-format/
* https://github.com/apache/parquet-java
* https://github.com/apache/parquet-format

What is RocksDB?
* RocksDB is a storage engine with key/value interface, where keys and values are arbitrary byte streams. It is a C++ library. It was developed at Facebook based on LevelDB and provides backwards-compatible support for LevelDB APIs.
* https://github.com/facebook/rocksdb/wiki

How to build OSM geometries (for multipolygons)?
* https://wiki.openstreetmap.org/wiki/Relation:multipolygon#Examples_in_XML
* https://osmcode.org/osm-testdata/
* https://github.com/GIScience/oshdb/blob/a196cc990a75fa35841ca0908f323c3c9fc06b9a/oshdb-util/src/main/java/org/heigit/ohsome/oshdb/util/geometry/OSHDBGeometryBuilderInternal.java#L469