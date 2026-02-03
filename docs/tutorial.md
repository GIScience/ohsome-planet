# Tutorial

In this tutorial we will use `ohsome-planet` to transform OSM data containing [OSM elements](https://wiki.openstreetmap.org/wiki/Element) (`node`, `way` and `relation`) into an ohsome-planet dataset containing [Simple Feature](https://en.wikipedia.org/wiki/Simple_Features) compliant geometries (E.g. `Point`, `Line` and `Polygon`) with OSM tags. Furthermore, we will enrich the ohsome-planet dataset with user and changeset information.

> [!NOTE]
> OSM provides data in PBF file format. `ohsome-planet` generates Parquet files.

In the first section (*I.*), we will download an OSM data extract (PBF), use the `ohsome-planet` command-line-interface (CLI) to generate an ohsome-planet dataset (Parquet) and explore those with [DuckDB](https://duckdb.org/) and [QGIS](https://qgis.org/).

In the second section (*II.*) we will enrich our ohsome-planet dataset (Parquet files) with user and changeset information.

## I. Create Parquet files containing OSM data

In this section, we will download an OSM data extract (PBF), use the `ohsome-planet` command-line-interface (CLI) to generate an ohsome-planet dataset (Parquet) and explore those with [DuckDB](https://duckdb.org/) and [QGIS](https://qgis.org/).

**First**, we need OSM data in PBF format.

We can get an OSM PBF etract from [Geofabrik](https://www.geofabrik.de/).
Let us download the [district of Karlsruhe](https://download.geofabrik.de/europe/germany/baden-wuerttemberg/karlsruhe-regbez.html), a city in Germany, to `karlsruhe-regbez-latest.osm.pbf`.

> [!NOTE]
> We can also download the OSM PBF file for the whole planet with history and without (latest) from the Planet [OSM server](https://planet.openstreetmap.org/). But processing this files takes a considerable amount of time. Thats why we stick to a smaller extract in this tutorial.

**Second**, we run ohsome-planet to transform the downloaded PBF file into Parquet files.

```sh
java -jar ohsome-planet-cli/target/ohsome-planet.jar \
    contributions \
    --pbf karlsruhe-regbez-latest.osm.pbf
```

Let us see the directory structure and files created by the previous command.

```sh
tree ohsome-planet/contributions

ohsome-planet/contributions/
├── history
│   ├── relation-0-history-contribs.parquet
│   └── ...
└── latest
    ├── node-0-latest-contribs.parquet
    ├── relation-0-latest-contribs.parquet
    ├── way-0-latest-contribs.parquet
    └── ...
```

With DuckDB, we can explore the data schema.

```sh
duckdb -s "DESCRIBE FROM read_parquet('ohsome-planet/contributions/*/*.parquet');"

┌───────────────────┬─────────────────────────────────────────────────────────┐
│    column_name    │                       column_type                       │
│      varchar      │                         varchar                         │
├───────────────────┼─────────────────────────────────────────────────────────┤
│ status            │ VARCHAR                                                 │
│ valid_from        │ TIMESTAMP WITH TIME ZONE                                │
│ valid_to          │ TIMESTAMP WITH TIME ZONE                                │
│ osm_type          │ VARCHAR                                                 │
│ osm_id            │ BIGINT                                                  │
│ osm_version       │ INTEGER                                                 │
│ osm_minor_version │ INTEGER                                                 │
│ osm_edits         │ INTEGER                                                 │
│ osm_last_edit     │ TIMESTAMP WITH TIME ZONE                                │
│ user              │ STRUCT(id INTEGER, "name" VARCHAR)                      │
│ tags              │ MAP(VARCHAR, VARCHAR)                                   │
│ tags_before       │ MAP(VARCHAR, VARCHAR)                                   │
│ changeset         │ STRUCT(id BIGINT, created_at TIMESTAMP WITH TIME ZONE…  │
│ bbox              │ STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DO…  │
│ centroid          │ STRUCT(x DOUBLE, y DOUBLE)                              │
│ xzcode            │ STRUCT("level" INTEGER, code BIGINT)                    │
│ geometry_type     │ VARCHAR                                                 │
│ geometry          │ BLOB                                                    │
│ area              │ DOUBLE                                                  │
│ area_delta        │ DOUBLE                                                  │
│ length            │ DOUBLE                                                  │
│ length_delta      │ DOUBLE                                                  │
│ contrib_type      │ VARCHAR                                                 │
│ refs_count        │ INTEGER                                                 │
│ refs              │ BIGINT[]                                                │
│ members_count     │ INTEGER                                                 │
│ members           │ STRUCT("type" VARCHAR, id BIGINT, "timestamp" TIMESTA…  │
│ countries         │ VARCHAR[]                                               │
│ build_time        │ BIGINT                                                  │
├───────────────────┴─────────────────────────────────────────────────────────┤
│ 29 rows                                                                     │
```

With QGIS we can explore the data on a map:

```sh
qgis ohsome-planet/contributions/latest/
```

> [!NOTE]
> Opening the dataset in QGIS, we notice "square" geometries. Those are the bounding boxes of relations with of more than 500 members. In those cases we skip `MultiPolygon` geometry building and fall back to `GeometryCollection`. 


## II. Enriche Parquet files with changeset information

In this section we will enrich our ohsome-planet dataset (Parquet files) with user and changeset information.

In previous section we create Parquet files with OSM contributions. Let us look at the `user` and `changeset` attributes of those Parquet files a bit closer.

```sh
duckdb -s "SELECT DISTINCT unnest(user) FROM read_parquet('ohsome-planet/contributions/*/*.parquet')"

┌───────┬─────────┐
│  id   │  name   │
│ int32 │ varchar │
├───────┼─────────┤
│     0 │         │
└───────┴─────────┘


duckdb -s "SELECT DISTINCT unnest(changeset) FROM read_parquet('ohsome-planet/contributions/*/*.parquet')"

┌───────┬──────────────────────────┬──────────────────────────┬───────────────────────┬───────────┬─────────┐
│  id   │        created_at        │        closed_at         │         tags          │ hashtags  │ editor  │
│ int64 │ timestamp with time zone │ timestamp with time zone │ map(varchar, varchar) │ varchar[] │ varchar │
├───────┼──────────────────────────┼──────────────────────────┼───────────────────────┼───────────┼─────────┤
│     0 │ 1970-01-01 01:00:00+01   │                          │ {}                    │ []        │         │
└───────┴──────────────────────────┴──────────────────────────┴───────────────────────┴───────────┴─────────┘
```

We notice that although user and changeset attributes are present, they do not have any values (or meaningless default values).

To enrich the parquet files with changeset information we will need to re-create the Parquet files, but this time provide user and changeset information in a database.

**First**, we create an empty PostGIS database with PostGIS extension enabled.

```sh
# We need those environment variables later on as well
export OHSOME_PLANET_DB=ohsomedb
export OHSOME_PLANET_DB_USER=ohsomedb
export OHSOME_PLANET_DB_PASSWORD=mysecretpassword
export OHSOME_PLANET_DB_SCHEMA=public
export OHSOME_PLANET_DB_POOLSIZE=10

docker run -d \
    --name ohsome_planet_changeset_db \
    -e POSTGRES_DB=$OHSOME_PLANET_DB \
    -e POSTGRES_USER=$OHSOME_PLANET_DB_USER \
    -e POSTGRES_PASSWORD=$OHSOME_PLANET_DB_PASSWORD \
    -p 5432:5432 \
    postgis/postgis
```

**Second**, we need an OSM changeset file. Geofabrik does not offer one and the Planet OSM server only provides one for the entire globe. Let us download [this changeset file](https://planet.openstreetmap.org/planet/) from the Planet OSM server to `changesets-latest.osm.bz2`.

**Third**, since we are only intrested in the district Karlsruhe let us use the tool [osmium](https://osmcode.org/osmium-tool/) to create an extract for Karlsruhe from the changeset file.

```sh
osmium changeset-filter \
    --bbox=7.959,48.300,9.604,49.664,
    --output=karlsruhe-regbez-changesets-latest.osm.bz2 \
    changesets-latest.osm.bz2
```

**Fourth**, we use ohsome-planet to parse the changeset file and import it into our PostGIS database.

```sh
java -jar ohsome-planet-cli/target/ohsome-planet.jar \
    changesets \
    --bz2 karlsruhe-regbez-changesets-latest.osm.bz2 \
    --changeset-db "jdbc:postgresql://localhost:5432/$OHSOME_PLANET_DB" \
    --create-tables
```

With psql we can explore the schema and check if data import has been successful.

```sh
PGPASSWORD=$OHSOME_PLANET_DB_PASSWORD psql \
    -h localhost \
    -p 5432 \
    -d $OHSOME_PLANET_DB \
    -U $OHSOME_PLANET_DB_USER \
    -c "\d changesets"

                       Table "public.changesets"
   Column   |           Type           | Collation | Nullable | Default
------------+--------------------------+-----------+----------+---------
 id         | bigint                   |           | not null |
 created_at | timestamp with time zone |           | not null |
 closed_at  | timestamp with time zone |           |          |
 tags       | jsonb                    |           | not null |
 hashtags   | character varying[]      |           | not null |
 user_id    | bigint                   |           | not null |
 user_name  | character varying        |           | not null |
 open       | boolean                  |           | not null |
 geom       | geometry(Polygon,4326)   |           |          |
Indexes:
    "changesets_id_key" UNIQUE CONSTRAINT, btree (id)


PGPASSWORD=$OHSOME_PLANET_DB_PASSWORD psql \
    -h localhost \
    -p 5432 \
    -d $OHSOME_PLANET_DB \
    -U $OHSOME_PLANET_DB_USER \

  count
--------
 167644
(1 row)   -c "SELECT COUNT(*) FROM changesets;"
```

**Fifth**, we need to run ohsome-planet again to transform a PBF file into Parquet files. 
But this time we need a PBF file with user and changeset ID to be able to join the data with our above create changeset database.
The previously downloaded PBF file does not contain user and changeset ID. The reason is stated by Geofabrik:

> The OpenStreetMap data files provided on this server do not contain the user names, user IDs and changeset IDs of the OSM objects because these fields are assumed to contain personal information about the OpenStreetMap contributors and are therefore subject to data protection regulations in the European Union. Extracts with full metadata are available to OpenStreetMap contributors only. (Geofabrik)

To get an OSM extract for the district of Karlsruhe with metadata (user and changeset ID), we need to [log in to Geofabrik](https://osm-internal.download.geofabrik.de/index.html) with our OSM account. Then [downlaod this PBF file](https://osm-internal.download.geofabrik.de/europe/germany/baden-wuerttemberg/karlsruhe-regbez.html) to `karlsruhe-regbez-latest-internal.osm.pbf`

**Finally**, we can run `ohsome-planet contributions` again to create our dataset:

```sh
rm -r ohsome-planet  # remove data directory
java -jar ohsome-planet-cli/target/ohsome-planet.jar \
    contributions \
    --pbf karlsruhe-regbez-latest-internal.osm.pbf \
    --changeset-db "jdbc:postgresql://localhost:5432/$OHSOME_PLANET_DB"
```

Now, we have the dataset with user and changeset information.

```sh
> duckdb -s "SELECT changeset.hashtags FROM read_parquet('ohsome-planet/contributions/*/*.parquet') WHERE changeset.hashtags != '[]' LIMIT 5;"
┌──────────────────────────┐
│         hashtags         │
│        varchar[]         │
├──────────────────────────┤
│ [QA]                     │
│ [maproulette]            │
│ [MapComplete, etymology] │
│ [MapComplete, etymology] │
│ [MapComplete, etymology] │
└──────────────────────────┘
```

Have fun exploring the dataset! To get started we [provide a couple of useful DuckDB queries](docs/useful_queries.md).
