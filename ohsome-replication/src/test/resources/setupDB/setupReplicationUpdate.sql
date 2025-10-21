CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE changesets
(
    changeset_id       int8 NOT NULL UNIQUE,
    created_at         timestamptz NOT NULL,
    closed_at          timestamptz NULL,
    tags               jsonb NOT NULL default '{}',
    hashtags           _varchar NULL,
    editor             varchar NULL,
    user_id            int8 NOT NULL,
    user_name          varchar NOT NULL,
    open               boolean NOT NULL
);


CREATE TABLE changeset_state
(
    id                 int NOT NULL UNIQUE,
    last_sequence      bigint NOT NULL,
    last_timestamp     timestamptz NOT NULL
);

INSERT INTO changesets (changeset_id, user_id, created_at, closed_at, open, user_name)
VALUES (34123412, 1231, '2025-10-20 09:10:36Z', null, true, 'bob');