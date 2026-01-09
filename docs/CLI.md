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
export OHSOME_PLANET_DB=
export OHSOME_PLANET_DB_USER=
export OHSOME_PLANET_DB_PASSWORD=
export OHSOME_PLANET_DB_SCHEMA=
export OHSOME_PLANET_DB_POOLSIZE=

java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --data /data/ohsome-planet/berlin \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --changeset-db "jdbc:postgresql://localhost:5432"
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
export OSM_OAUTH_TOKEN=your_token

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
    --include-tags "highway,building,landuse"
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

The flag `--schema` will initialize the data schema, in this case a table called `changesets` and another called
`changeset_state`. Using the flag `--overwrite` will truncate these tables first before the new insert process starts.

The changesets schema looks like this:
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


## Replication

The postgres connection can be customized with the same environment variables as for the changesets command.


This command can be used to either update changesets or contributions individually, or update both at the same time.
If you want to only update changesets you need to supply all arguments that concern changesets and use the flag `--jcs`,
similarly you can do the same for contributions and use `--jcb`.



Some additional optional parameters are available, which can be seen using the `--help` command. Among others
the `--continuous` flag can be used to make the update process run as a continuous service, and `--replication-changesets`
can be used to set a custom changeset replication source.