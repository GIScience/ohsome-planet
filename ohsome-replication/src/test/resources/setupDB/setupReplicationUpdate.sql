CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE changesets
(
    changeset_id       int8 NOT NULL UNIQUE,
    created_at         timestamptz NOT NULL,
    closed_at          timestamptz NULL,
    tags               jsonb NOT NULL default '{}',
    hashtags           _varchar NOT NULL default '{}',
    editor             varchar NULL,
    user_id            int8 NOT NULL,
    user_name          varchar NOT NULL,
    open               boolean NOT NULL,
    num_changes        int8 NOT NULL default 0
);


CREATE TABLE changeset_state
(
    id                 int NOT NULL UNIQUE,
    last_sequence      bigint NOT NULL,
    last_timestamp     timestamptz NOT NULL
);

INSERT INTO changesets (changeset_id, created_at, closed_at, open, user_id, user_name, tags, hashtags)
VALUES (34123412, '2025-10-20 09:10:36Z', null, true, 1234, 'bob',
        '{"comment": "this is a changeset for testing #ohsome-planet", "created_by": "ohsome-planet"}'::jsonb,
           '{"ohsome-planet"}');