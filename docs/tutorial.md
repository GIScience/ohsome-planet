# Tutorial

In this tutorial, we will first create (Geo)Parquet files from OSM PBF files.
After that, we will look into how to enrich those Parquet with changeset data.


## I Create Parquet files with OSM contributions

In this section, we will create Parquet files with OSM data and explore this data with [DuckDB](https://duckdb.org/) and [QGIS](https://qgis.org/).

**First**, we need OSM data in PBF format.

We can get an OSM PBF extract from [Geofabrik](https://www.geofabrik.de/).
Let us download the [district of Karlsruhe](https://download.geofabrik.de/europe/germany/baden-wuerttemberg/karlsruhe-regbez.html), a city in Germany, to `karlsruhe-regbez-latest.osm.pbf`.

> [!NOTE]
> We can also download the OSM PBF file for the whole planet with history and without (latest) from the Planet [OSM server](https://planet.openstreetmap.org/).

**Second**, we run ohsome-planet to transform the downloaded PBF file into Parquet files.

```sh
mkdir data
java -jar ohsome-planet-cli/target/ohsome-planet.jar \
    contributions \
    --data data/ \
    --pbf karlsruhe-regbez-latest.osm.pbf
```

Let us see the directory structure and files created by the previous command.

```sh
tree data/contributions

data/contributions/
├── history
│   ├── relation-0-history-contribs.parquet
│   └── ...
└── latest
    ├── node-0-latest-contribs.parquet
    ├── relation-0-latest-contribs.parquet
    ├── way-0-latest-contribs.parquet
    └── ...
```

Notice ... (TODO: Explain files created, TODO: Why history)

With DuckDB, we can explore the data schema.

```sh
duckdb -s "DESCRIBE FROM read_parquet('data/contributions/*/*.parquet');"

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
qgis data/contributions/latest/
```


## II Enriche Parquet files with changeset information

In this section we will enriche our parquet files with user and changeset information.

In previous section we create Parquet files with OSM contributions. Let us look at the `user` and `changeset` attributes of those Parquet files a bit closer.

```sh
duckdb -s "SELECT DISTINCT unnest(user) FROM read_parquet('data/contributions/*/*.parquet')"

┌───────┬─────────┐
│  id   │  name   │
│ int32 │ varchar │
├───────┼─────────┤
│     0 │         │
└───────┴─────────┘


duckdb -s "SELECT DISTINCT unnest(changeset) FROM read_parquet('data/contributions/*/*.parquet') LIMIT 1"

┌───────┬──────────────────────────┬──────────────────────────┬───────────────────────┬───────────┬─────────┐
│  id   │        created_at        │        closed_at         │         tags          │ hashtags  │ editor  │
│ int64 │ timestamp with time zone │ timestamp with time zone │ map(varchar, varchar) │ varchar[] │ varchar │
├───────┼──────────────────────────┼──────────────────────────┼───────────────────────┼───────────┼─────────┤
│     0 │ 1970-01-01 01:00:00+01   │                          │ {}                    │ []        │         │
└───────┴──────────────────────────┴──────────────────────────┴───────────────────────┴───────────┴─────────┘
```

We notice that although user and changeset attributes are present, they do not have any values (or meaningless default values).

To enrich the parquet files with changeset informations we will need to re-create the Parquet files, but this time provide user and changeset information through a database.

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

**Second**, we download the [changeset file](https://planet.openstreetmap.org/planet/) from the Planet OSM server to `changesets-latest.osm.bz2`. This changeset file covers the etire globe.

**Third**, since we are only intrested in the district Karlsruhe let use use the tool [osmium](https://osmcode.org/osmium-tool/) to create an extract for Karlsruhe from the changeset file.

```sh
osmium changeset-filter \
    --bbox=8.319,48.962,8.475,49.037 \
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

With psql we can explore the schema.

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
```

**Fifth**, we run ohsome-planet again to transform the previously downloaded PBF file (see section 1) into Parquet files. But this time we provide it with the above create changeset database. Aditionaly, we tell ohsome-planet that it is okay to overwrite existing data.

```sh
mkdir data
java -jar ohsome-planet-cli/target/ohsome-planet.jar \
    contributions \
    --data data/ \
    --pbf karlsruhe-regbez-latest.osm.pbf \
    --changeset-db "jdbc:postgresql://localhost:5432/$OHSOME_PLANET_DB" \
    --overwrite
```
