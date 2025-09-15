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

INSERT INTO changeset_state
VALUES (0, 10020, '2021-12-12 09:10:15');

INSERT INTO changesets (changeset_id, user_id, created_at, closed_at, open, user_name)
VALUES (111, 1231, '2021-12-12 09:10:15', null, true, 'bob'),
       (1231, 1231, '2022-12-12 09:10:15', null, true, 'bob'),
       (34123412, 1231, '2022-12-12 09:10:15', null, true, 'bob');