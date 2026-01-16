# CLI Documentation

Here we provide additional information for using the command line interface (CLI).

## Contributions

### Write to S3
Write Parquet files directly into S3 object store.
This will temporarily write Parquet files to your disk and subsequently move them into the S3 path.

```shell
export S3_KEY_ID=your_s3_key_id
export S3_SECRET=your_s3_secret
export S3_ENDPOINT=your_s3_endpoint
export S3_REGION=your_s3_region

java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --parquet-data "s3://heigit-ohsome-planet/data/v1/berlin/contributions/"
```


### Join Country Codes
By passing the parameter `--country-file` you can perform a spatial join to enrich OSM contributions with country codes.
Passing this option will populate the `countries` attribute in the Parquet files.

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --country-file /data/world.csv
```

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

### Join Changeset Tags
By passing the parameter `--changeset-db` you can join OSM changeset information.

```shell
export OHSOME_PLANET_DB_USER=your_user
export OHSOME_PLANET_DB_PASSWORD=your_password
export OHSOME_PLANET_DB_SCHEMA=public
export OHSOME_PLANET_DB_POOLSIZE=100

java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --changeset-db "jdbc:postgresql://localhost:5432/postgres"
```

You can pass the database connection also as JDBC URL including the credentials, e.g. `jdbc:postgresql://localhost:5432/postgres?user=your_user&password=your_password`.
You can set up this database with ohsome-planet as well using the `changesets` command.
See below for further details.

The changeset join will populate the `changeset` struct attribute in the parquet files with the following information:
- `closed_at`
- `tags`
- `hashtags`
- `editor`
- `numChanges`


### Initialize for Replication
In case you are planning to derive minutely-derived updates, then you have to first run the `contributions` command of ohsome-planet together with the `--replication-endpoint` parameter.

Write Parquet files and initialize the source data needed for replication from OSM planet server.

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --replication-endpoint "https://planet.osm.org/replication/minute/"
```

Write Parquet files and initialize the source data needed for replication from GeoFabrik replication server.
You can get this token using their [OSM OAuth Cookie Client](https://github.com/geofabrik/sendfile_osm_oauth_protector/blob/master/doc/client.md) python script.

```shell
export OSM_REPLICATION_ENDPOINT_COOKIE=your_oauth_token

java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --replication-endpoint "https://osm-internal.download.geofabrik.de/europe/germany/berlin-updates/"
```

In both cases your data folder will contain additional information.
The replication folder will be used to derive minutely updates with the `replication` command of ohsome-planet.

```text
/data/ohsome-planet/berlin
├── contributions
│   ├── history
│   │   ├── node-0-history.parquet
│   │   ├── ...
│   │   ├── way-0-history.parquet
│   │   ├── ...
│   │   ├── relation-0-history.parquet
│   │   └── ...
│   ├── latest
│   │   ├── node-0-latest.parquet
│   │   ├── ...
│   │   ├── way-0-latest.parquet
│   │   ├── ...
│   │   ├── relation-0-latest.parquet
│   │   ├── ...
│   └── state.txt
└── replication
    ├── node_relations (rocksDB)
    ├── nodes (rocksDB)
    ├── node_ways (rocksDB)
    ├── relations (rocksDB)
    ├── way_relations (rocksDB)
    └── ways (rocksDB)
```

### Filter Relations by OSM Tag Keys
At the moment, there is only limited support for tag filtering.
By passing the `--include-tags` parameter you can specify a comma separated list of OSM tag keys, e.g. `highway,building,landuse`.

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --filter-relation-tag-keys "highway,building,landuse"
```

These tag keys will be used to filter OSM relations only.
Currently, filtering for OSM nodes or ways is not implemented.
In case you have more complex tag filtering needs, please refer to the [osmium documentation](https://docs.osmcode.org/osmium/latest/osmium-tags-filter.html) in order to prepare the input OSM pbf file.
We are planning to add more tag filtering options in the future.


## Changesets

`--changeset-db` requires the path to a running postgres instance. You can configure the postgres connection by using
the following environment variables:

```
OHSOME_PLANET_DB_USER=your_user
OHSOME_PLANET_DB_PASSWORD=your_pw
OHSOME_PLANET_DB_POOLSIZE=32
OHSOME_PLANET_DB_SCHEMA=public
```

The flag `--create-tables` will initialize the data schema, in this case a table called `changesets` and another called
`changeset_state`. Using the flag `--overwrite` will truncate these tables first before the new insert process starts.

The changesets table schema looks like this:
```shell
    id                 int8 NOT NULL UNIQUE,
    created_at         timestamptz NOT NULL,
    closed_at          timestamptz NULL,
    tags               jsonb NOT NULL,
    hashtags           _varchar NOT NULL,
    user_id            int8 NOT NULL,
    user_name          varchar NOT NULL,
    open               boolean NOT NULL,
    geom               geometry(polygon, 4326) NULL
```


## Replications
The `replications` command can be used to either update changesets or contributions individually, or update both at the same time.
If you want to only update changesets you need to supply all arguments that concern changesets and use the flag `--just-changeset`,
similarly you can do the same for contributions and use `--just-contribution`.

Update only changesets:
```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar replications \
    --changeset-db "jdbc:postgresql://localhost:5432/postgres?user=your_user&password=your_password" \
    --just-changesets \
    -v
```


Before running replication for contributions you need to make sure to initialize all files needed with the `contributions` command.
Then, update only contributions (no changeset join happens here) with this command:

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar replications \
    --data path/to/data \
    --just-contributions \
    --size 60 \
    -v
```

The optional `--size` parameter can be useful in the non-continue mode.
The parameter defines the number of `.osc` files that will be batched.
For example, `--size=60` will update 60 files (usually 60 minutes) and then stop the process.


## Debug

### PBF file information

You can use `debug fileinfo` to print the header for an OSM .pbf file.
This mimics the functionality of Osmium's `fileinfo` command.

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar debug fileinfo \
  --pbf /data/osm/berlin-internal.osh.pbf
```

Will produce something like this.

````text
File:
  Name: /data/osm/berlin-internal.osh.pbf
  Size: 390367622
Header:
  Bounding_Boxes: BBox{left=13.08283, right=13.762245, top=52.6783, bottom=52.33446}
  History: true
  Generator: osmium/1.16.0
  Replication:
    Base_Url: null
    Sequence_Number: 0
    Timestamp: 1970-01-01T00:00:00Z
  Features:
  - HistoricalInformation
  - OsmSchema-V0.6
  - DenseNodes
  - Sort.Type_then_ID
read blocks 100% │█████████████████████████████████│ 372/372 MiB (0:00:00 / 0:00:00) 2905 blocks
Blobs by type:
  Nodes: 2163 | Ways: 674 | Relations: 68
````

### Inspect Replication Store
The `debug replication-store` command can be used to show the latest state from the individual replication RocksDBs.
This is useful when checking for an individual OSM element in replication store which you can pass by OSM id and OSM type. Use `n/OSM_ID` for nodes, `w/OSM_ID` for ways and `r/OSM_ID` for relations.

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar debug replication-store \
  --data path/to/data \
  n/270418052 w/721933838   
```

Specify the OSM elements like `n/270418052` or `w/721933838`. 

The result will show all available information in the replication-store for these elements.
If a value is `-1`, then this means that no information is stored, e.g. for changeset or user ID.

```text
n/270418052:
  OSMNode[id=270418052, version=7, timestamp=2013-01-06T10:34:49Z, changeset=-1, userId=-1, user=, visible=true, tags={}, lon=8.6761206, lat=49.4193076]
  ways:[34126010, 849049869, 721933838]
  rels:null
w/721933838:
  OSMWay[id=721933838, version=7, timestamp=2023-08-11T11:14:42Z, changeset=-1, userId=-1, user=, visible=true, tags={sidewalk=separate, surface=asphalt, zone:traffic=DE:urban, maxspeed=50, name:etymology:wikidata=Q64, cycleway:left=no, oneway=yes, zone:maxspeed=DE:urban, smoothness=good, cycleway:right=separate, lit=yes, name=Berliner Straße, lanes=2, highway=primary, parking:lane:both=no, priority_road=designated}, refs=[270418052, 6772131501], minorVersion=0, edits=7, lons=null, lats=null]
  rels:[3003370, 3123494, 399048]
```

