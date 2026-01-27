# ohsome-planet

[![Build Status](https://jenkins.heigit.org/buildStatus/icon?job=ohsome-planet/main)](https://jenkins.heigit.org/job/ohsome-planet/job/main/)
[![Sonarcloud Status](https://sonarcloud.io/api/project_badges/measure?project=org.heigit.ohsome:ohsome-planet&metric=alert_status)](https://sonarcloud.io/dashboard?id=org.heigit.ohsome:ohsome-planet)
[![LICENSE](https://img.shields.io/github/license/GIScience/ohsome-planet)](LICENSE)
[![status: active](https://github.com/GIScience/badges/raw/master/status/active.svg)](https://github.com/GIScience/badges#active)

The ohsome-planet tool can be used to:
1. Transform OSM (history) PBF files into Parquet format with native GEO support.
2. Turn an OSM changeset file (`osm.bz2`) into a PostgreSQL database table.
3. Keep both datasets up-to-date by ingesting OSM planet replication files.

ohsome-planet creates the actual OSM elements geometries for nodes, ways and relations.
It enriches each element with changeset data such as hastags, OSM editor or username to the elements. Additionaly it is possibly to add country codes to each element by providing a boundary dataset.

The output of ohsome-planet can be used to perform a wide range of geospatial analyses with tools such as DuckDB, Python GeoPandas or QGIS. Its also possible to display the data directly on a map and explore it.


## Installation

Installation requires Java 21.

First, clone the repository and its submodules. Then, build it with Maven.

```shell
git clone --recurse-submodules https://github.com/GIScience/ohsome-planet.git
cd ohsome-planet
./mvnw clean package -DskipTests
```


## Usage

There are three modes to run ohsome-planet.

1. **Contributions**: OSM Extract (`.pbf`) 󰁔 Parquet
2. **Changesets**: OSM Changesets (`.bz2`) 󰁔 PostgreSQL
3. **Replication**: OSM Replication Files (`.osc`) 󰁔 Parquet / PostgreSQL


### Contributions (Parquet)

> Transform OSM (history/latest) `.pbf` file into Parquet format.

You can download the latest or history OSM extract (`osm.pbf`) for the whole planet from the [OSM Planet server](https://planet.openstreetmap.org/pbf/full-history/) or for small regions from [Geofabrik](https://osm-internal.download.geofabrik.de/).

To process a given `.pbf` file, provide it in the `--pbf` parameter in the following example.
Here we use a history file for Berlin obtained from GeoFabrik. 

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --changeset-db "jdbc:postgresql://localhost:5432/postgres?user=your_user&password=your_password" \
    --country-file /data/world.csv \
    --parallel 8 \
    --overwrite 
```

The parameters `--parallel`, `--country-file`, `--changeset-db` and `--overwrite` are optional. Find more detailed information on usage here: [docs/CLI.md](docs/CLI.md#contributions). To see all available parameters, call the tool with `--help` parameter.

When using a history PBF file, the output files are split into `history` and `latest` contributions.
All contributions which are a) not deleted and b) visible in OSM at the timestamp of the extract are considered as `latest`.
The remaining contributions, e.g. deleted or old versions, are considered as `history`.
The number of threads (`--parallel` parameter) defines the number of files which will be created.

```text
/data/ohsome-planet/berlin
└── contributions
    ├── history
    │   ├── node-0-history.parquet
    │   ├── ...
    │   ├── way-0-history.parquet
    │   ├── ...
    │   ├── relation-0-history.parquet
    │   └── ...
    └── latest
        ├── node-0-latest.parquet
        ├── ...
        ├── way-0-latest.parquet
        ├── ...
        ├── relation-0-latest.parquet
        └── ...
```


### Changesets (PostgreSQL)

> Import OSM changesets `.bz2` file to PostgreSQL.

First, create an empty PostgreSQL database with PostGIS extension or provide a connection to an existing database. For instance, you can set it up like this.

```shell
export OHSOME_PLANET_DB_USER=your_password
export OHSOME_PLANET_DB_PASSWORD=your_user

docker run -d \
    --name ohsome_planet_changeset_db \
    -e POSTGRES_PASSWORD=$OHSOME_PLANET_DB_PASSWORD \
    -e POSTGRES_USER=$OHSOME_PLANET_DB_USER \
    -p 5432:5432 \
    postgis/postgis
```

Second, download the [full changeset file](https://planet.openstreetmap.org/planet/) from the OSM Planet server. If you want to clip the extent to a smaller region, you can use the `changeset-filter` command of the [osmium library](https://docs.osmcode.org/osmium/latest/osmium-changeset-filter.html). This might take a few minutes. Currently, there is no provider for pre-processed or regional changeset file extracts. 

```shell
osmium changeset-filter \
    --bbox=8.319,48.962,8.475,49.037 \
    changesets-latest.osm.bz2 \
    changesets-latest-karlsruhe.osm.bz2 
```

Then, process the OSM changesets `.bz2` file like in the following example.

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar changesets \
    --bz2 data/changesets-latest-karlsruhe.osm.bz2 \
    --changeset-db "jdbc:postgresql://localhost:5432/postgres?user=your_user&password=your_password" \
    --create-tables \
    --overwrite
```

The parameters `--create-tables` and `--overwrite` are optional. Find more detailed information on usage here: [docs/CLI.md](docs/CLI.md#changesets). To see all available parameters, call the tool with `--help` parameter.


### Replications (Parquet / PostgreSQL)

> Transform OSM replication .osc files into parquet format. 
>
> Keep changeset PostgreSQL database up-to-date.

The ohsome-planet tool can also be used to generate updates from the replication files provided by the 
[OSM Planet server](https://planet.openstreetmap.org/replication/).
GeoFabrik also provides updates for regional extracts.

If you want to update both datasets your command should look like this: 

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar replications \
    --data path/to/data \
    --changeset-db "jdbc:postgresql://localhost:5432/postgres?user=your_user&password=your_password" \
    --parallel 8 \
    --country-file data/world.csv \
    --parquet-data path/to/parquet/output/ \
    --continue
```

Just like for the `contributions` command you can use the optional parameters `--parallel`, `--country-file`, `--parquet-data` arguments here as well.
The optional `--continue` flag can be used to make the update process run as a continuous service, which will wait and fetch new changes from the OSM planet server.
If you want to only update changesets you can use the `--just-changesets` flag. You can do the same for contributions with `--just-contributions`.

Find more detailed information on usage here: [docs/CLI.md](docs/CLI.md#replication). To see all available parameters, call the tool with `--help` parameter.

Contributions will be written as Parquet files matching those found in the replication source.
This mimics the structure of the [OSM Planet Server](https://planet.osm.org/replication/minute/).
You can use the top level state files (`state.txt` or `state.csv`) to find the most recent sequence number.

```text
/data/ohsome-planet/berlin
└── updates
    ├── 006
    │   ├── 942
    │   │   ├── 650.opc.parquet
    │   │   ├── 650.state.txt
    │   │   ├── ...
    │   │   ├── 001.opc.parquet
    │   │   └── 001.state.txt
    │   ├── 941
    │   ├── ...
    │   └── 001
    ├── state.csv
    └── state.txt
```


## Inspect Results

You can inspect your results easily using [DuckDB](https://duckdb.org/docs/installation).
Take a look at our collection of [useful queries](docs/useful_queries.md) to find many analysis examples.

```sql
-- list all columns
DESCRIBE FROM read_parquet('contributions/*/*.parquet');

-- result
┌───────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
│    column_name    │                                                                                  column_type                                                                                   │  null   │   key   │ default │  extra  │
│      varchar      │                                                                                    varchar                                                                                     │ varchar │ varchar │ varchar │ varchar │
├───────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼─────────┼─────────┼─────────┼─────────┤
│ status            │ VARCHAR                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ valid_from        │ TIMESTAMP WITH TIME ZONE                                                                                                                                                       │ YES     │ NULL    │ NULL    │ NULL    │
│ valid_to          │ TIMESTAMP WITH TIME ZONE                                                                                                                                                       │ YES     │ NULL    │ NULL    │ NULL    │
│ osm_type          │ VARCHAR                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ osm_id            │ BIGINT                                                                                                                                                                         │ YES     │ NULL    │ NULL    │ NULL    │
│ osm_version       │ INTEGER                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ osm_minor_version │ INTEGER                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ osm_edits         │ INTEGER                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ osm_last_edit     │ TIMESTAMP WITH TIME ZONE                                                                                                                                                       │ YES     │ NULL    │ NULL    │ NULL    │
│ user              │ STRUCT(id INTEGER, "name" VARCHAR)                                                                                                                                             │ YES     │ NULL    │ NULL    │ NULL    │
│ tags              │ MAP(VARCHAR, VARCHAR)                                                                                                                                                          │ YES     │ NULL    │ NULL    │ NULL    │
│ tags_before       │ MAP(VARCHAR, VARCHAR)                                                                                                                                                          │ YES     │ NULL    │ NULL    │ NULL    │
│ changeset         │ STRUCT(id BIGINT, created_at TIMESTAMP WITH TIME ZONE, closed_at TIMESTAMP WITH TIME ZONE, tags MAP(VARCHAR, VARCHAR), hashtags VARCHAR[], editor VARCHAR, numChanges INTEGER) │ YES     │ NULL    │ NULL    │ NULL    │
│ bbox              │ STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)                                                                                                                     │ YES     │ NULL    │ NULL    │ NULL    │
│ centroid          │ STRUCT(x DOUBLE, y DOUBLE)                                                                                                                                                     │ YES     │ NULL    │ NULL    │ NULL    │
│ xzcode            │ STRUCT("level" INTEGER, code BIGINT)                                                                                                                                           │ YES     │ NULL    │ NULL    │ NULL    │
│ geometry_type     │ VARCHAR                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ geometry          │ GEOMETRY                                                                                                                                                                       │ YES     │ NULL    │ NULL    │ NULL    │
│ area              │ DOUBLE                                                                                                                                                                         │ YES     │ NULL    │ NULL    │ NULL    │
│ area_delta        │ DOUBLE                                                                                                                                                                         │ YES     │ NULL    │ NULL    │ NULL    │
│ length            │ DOUBLE                                                                                                                                                                         │ YES     │ NULL    │ NULL    │ NULL    │
│ length_delta      │ DOUBLE                                                                                                                                                                         │ YES     │ NULL    │ NULL    │ NULL    │
│ contrib_type      │ VARCHAR                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ refs_count        │ INTEGER                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ refs              │ BIGINT[]                                                                                                                                                                       │ YES     │ NULL    │ NULL    │ NULL    │
│ members_count     │ INTEGER                                                                                                                                                                        │ YES     │ NULL    │ NULL    │ NULL    │
│ members           │ STRUCT("type" VARCHAR, id BIGINT, "timestamp" TIMESTAMP WITH TIME ZONE, "role" VARCHAR, geometry_type VARCHAR, geometry BLOB)[]                           │ YES     │ NULL    │ NULL    │ NULL    │
│ countries         │ VARCHAR[]                                                                                                                                                                      │ YES     │ NULL    │ NULL    │ NULL    │
│ build_time        │ BIGINT                                                                                                                                                                         │ YES     │ NULL    │ NULL    │ NULL    │
├───────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┤
│ 29 rows                                                                                                                                                                                                                          6 columns │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


## Getting Started as Developer

This is a list of resources that you might want to take a look at to get a better understanding of the core concepts used for this project. 
In general, you should gain some understanding of the raw OSM (history) data format and know how to build geometries from nodes, ways and relations.
Furthermore, knowledge about (Geo)Parquet files is useful as well.

What is the OSM PBF File Format?
* https://wiki.openstreetmap.org/wiki/PBF_Format
* History PBF files for smaller regions: [Geofabrik](https://osm-internal.download.geofabrik.de/)
* History or latest PBF files for the whole planet: [OSM Planet](https://planet.openstreetmap.org/planet/full-history/)

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


## Further Notes

* For relations that consist of more than 500 members we skip `MultiPolygon` geometry building and fall back to `GeometryCollection`. Check `MEMBERS_THRESHOLD` in `ohsome-contributions/src/main/java/org/heigit/ohsome/contributions/contrib/ContributionGeometry.java`.
* For contributions with status `deleted` we use the geometry of the previous version. This allows you to spatially filter also for deleted elements, e.g. by bounding box. In the sense of OSM deleted elements do not have any geometry.
