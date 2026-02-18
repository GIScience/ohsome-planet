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
It enriches each element with changeset data such as hastags, OSM editor or username.
Additionally it is possibly to add country ISO codes to each element by providing a boundary dataset.

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

To see the help page of the ohsome-planet CLI run:
```sh
java -jar ohsome-planet-cli/target/ohsome-planet.jar --help
```

## Tutorial

[This tutorial](docs/tutorial.md) shows how to use `ohsome-planet` to transform OSM data containing [OSM elements](https://wiki.openstreetmap.org/wiki/Element) (`node`, `way` and `relation`) into an ohsome-planet dataset containing [Simple Feature](https://en.wikipedia.org/wiki/Simple_Features) compliant geometries (E.g. `Point`, `Line` and `Polygon`) with OSM tags, user and changeset information in Parquet format.

## Frequently Asked Questions (FAQ)

For a list of *frequently asked questions* please [see this document](docs/faq.md).

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
