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
The parameters `--country-file`, `--output` and `--overwrite` are optional.
To see all available parameters, call the tool with `--help` parameter.

### Country Data
By passing the parameter `--country-file` you can perform a spatial join to enrich OSM contributions with country codes.
The country file should be provided in `.csv` format.
Geometries should we represented as `WKT` (well-known text) string.
The current version only supports `POLYGON` or `MULTIPOLYGON` geometries.

Basically, the file should look like this:
```
id;wkt
DEU;POLYGON ((7.954102 49.781264, 11.118164 49.781264, 11.118164 51.563412, 7.954102 51.563412, 7.954102 49.781264))
FRA;POLYGON ((1.186523 45.058001, 4.833984 45.058001, 4.833984 48.545705, 1.186523 48.545705, 1.186523 45.058001))
ITA;POLYGON ((10.766602 41.211722, 14.985352 41.211722, 14.985352 44.024422, 10.766602 44.024422, 10.766602 41.211722))
```

### Changesets
We will add the functionality to join OSM changeset metadata soon. Stay tuned!

## Output Structure

When using a history PBF file, the output files are split into `history` and `latest` contributions. 
All contributions which are a) not deleted and b) visible in OSM at the timestamp of the extract are considered as `latest`.
The remaining contributions, e.g. deleted or old versions, are considered as `history`.

```
out-karlruhe
├── contributions
│   ├── history
│   │   ├── node-0-163811-history.parquet
│   │   ├── ...
│   │   ├── way-0-2496473-history.parquet
│   │   ├── ...
│   │   ├── relation-0-12345-history.parquet
│   │   └── ...
│   └── latest
│       ├── node-0-163811-latest.parquet
│       ├── ...
│       ├── way-0-2496473-latest.parquet
│       ├── ...
│       ├── relation-0-12345-latest.parquet
│       └── ...
├── minorNodes (rocksdb)
└── minorWays (rocksdb)
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
│ osm_edits         │ INTEGER                                                                                                                                    │ YES     │         │         │         │
│ osm_last_edit     │ TIMESTAMP WITH TIME ZONE                                                                                                                   │ YES     │         │         │         │
│ user              │ STRUCT(id INTEGER, "name" VARCHAR)                                                                                                         │ YES     │         │         │         │
│ tags              │ MAP(VARCHAR, VARCHAR)                                                                                                                      │ YES     │         │         │         │
│ tags_before       │ MAP(VARCHAR, VARCHAR)                                                                                                                      │ YES     │         │         │         │
│ changeset         │ STRUCT(id BIGINT, created_at TIMESTAMP WITH TIME ZONE, closed_at TIMESTAMP WITH TIME ZONE, tags MAP(VARCHAR, VARCHAR), hashtags VARCHAR[]) │ YES     │         │         │         │
│ bbox              │ STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)                                                                                 │ YES     │         │         │         │
│ centroid          │ STRUCT(x DOUBLE, y DOUBLE)                                                                                                                 │ YES     │         │         │         │
│ geometry_type     │ VARCHAR                                                                                                                                    │ YES     │         │         │         │
│ geometry          │ BLOB                                                                                                                                       │ YES     │         │         │         │
│ area              │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ area_delta        │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ length            │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ length_delta      │ DOUBLE                                                                                                                                     │ YES     │         │         │         │
│ contrib_type      │ VARCHAR                                                                                                                                    │ YES     │         │         │         │
│ refs              │ BIGINT[]                                                                                                                                   │ YES     │         │         │         │
│ members           │ STRUCT("type" VARCHAR, id BIGINT, "role" VARCHAR, geometry_type VARCHAR, geometry BLOB)[]                                                  │ YES     │         │         │         │
│ country_iso       │ VARCHAR[]                                                                                                                                  │ YES     │         │         │         │
│ build_time        │ BIGINT                                                                                                                                     │ YES     │         │         │         │
├───────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┤
│ 26 rows                                                                                                                                                                                      6 columns │
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
