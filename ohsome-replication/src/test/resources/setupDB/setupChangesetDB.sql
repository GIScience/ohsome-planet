CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE changesets
(
    id                 int8 NOT NULL UNIQUE,
    created_at         timestamptz NOT NULL,
    closed_at          timestamptz NULL,
    tags               jsonb NOT NULL,
    hashtags           _varchar NOT NULL,
    user_id            int8 NOT NULL,
    user_name          varchar NOT NULL,
    open               boolean NOT NULL,
    geom               geometry(polygon, 4326) NULL
);


CREATE TABLE changeset_state
(
    last_sequence      bigint NOT NULL,
    last_timestamp     timestamptz NOT NULL
);