# CLI Documentation

Here we provide additional information for using the command line interface (CLI).

## Contributions


Write Parquet files directly into S3 object store.
* maybe this should use `s3:` instead of `minio:`
* 

```shell
export S3_KEY_ID=your_s3_key_id
export S3_SECRET=your_s3_secret

java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --output "https://storage.heigit.org/heigit-ohsome-planet/data/v1/berlin/contributions/2026-01-09/" \
    --overwrite 
```


Setup initial data needed to start replication.

```shell
java -jar ohsome-planet-cli/target/ohsome-planet.jar contributions \
    --pbf /data/osm/berlin-internal.osh.pbf \
    --parallel 8 \
    --overwrite 
```




parameters:
* `--data` (mandatory): local path where all output (including temporary files, replication files, parquet files) will be stored. this path will be used when you run `replication update` to find the replication sub-directory.

* `--keep-temp-data` (optional): boolean, temp files will not be deleted after process finishes

* `--parquet-data` (optional): `/data/out-test/` or `s3://heigit-ohsome-planet/data/v1/benni-test`

* `--init-replication` (optional): boolean. this will create the replication files in the data directory



Full output structure
```
ohsome-planet -- output data directory
├── contributions -- parquet-data
│   ├── history
│   │   ├── node-0-172539-history-contribs.parquet
│   │   ├── ...
│   │   ├── relation-0-history-contribs.parquet
│   │   ├── ...
│   │   ├── way-0-3981228-history-contribs.parquet
│   │   ├── ...
│   └── latest
│       ├── node-0-172539-latest-contribs.parquet
│       ├── ...
│       ├── relation-0-latest-contribs.parquet
│       ├── ...
│       ├── way-0-3981228-latest-contribs.parquet
│       ├── ...
├── replication -- there is no param to set this
│   ├── node_relations (rocksDB)
│   ├── nodes (rocksDB)
│   ├── node_ways (rocksDB)
│   ├── relations (rocksDB)
│   ├── way_relations (rocksDB)
│   └── ways (rocksDB)
└── tmp -- there is no param to set this, but you can use --keep-temp-data
    ├── minorNodes (rocksDB)
    ├── minorWays (rocksDB)
    └── progress
```


#### Country Data
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

Passing this option will populate the `countries` attribute in the parquet files.

#### Changesets
By passing the parameter `--changeset-db` you can join OSM changeset information.
It is expected that you pass the database connection as JDBC URL, e.g. `jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD`.
Currently, ohsome-planet can connect to a database following the schema of [ChangesetMD](https://github.com/ToeBee/ChangesetMD).

The changeset join will populate the `changeset` struct attribute in the parquet files with the following information:
- `closed_at`
- `tags`
- `hashtags`
- `editor`
- `numChanges`


#### Tag Filtering
At the moment, there is only limited support for tag filtering.
By passing the `--include-tags` parameter you can specify a comma separated list of OSM tag keys, e.g. `highway,building,landuse`.
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