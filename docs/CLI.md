# CLI Documentation

Here we provide additional information for using the command line interface (CLI).

## Contributions

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
