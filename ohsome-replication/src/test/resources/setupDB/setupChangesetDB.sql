CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE osm_changeset
(
    id          bigint,
    user_id     bigint,
    created_at  timestamp without time zone,
    closed_at   timestamp without time zone,
    open        boolean,
    num_changes integer,
    user_name   varchar(255),
    tags        hstore,
    PRIMARY KEY (id)
);

CREATE TABLE osm_changeset_comment
(
    comment_changeset_id bigint                      not null,
    comment_user_id      bigint                      not null,
    comment_user_name    varchar(255)                not null,
    comment_date         timestamp without time zone not null,
    comment_text         text                        not null
);

CREATE TABLE osm_changeset_state
(
    id                 int,
    last_sequence      bigint,
    last_timestamp     timestamp without time zone
);

INSERT INTO osm_changeset_state
VALUES (0, 10020, '2021-12-12 09:10:15');

INSERT INTO osm_changeset
VALUES (111, 1231, '2021-12-12 09:10:15', null, true, 12, 'bob', null),
       (1231, 1231, '2022-12-12 09:10:15', null, true, 12, 'bob', null),
       (34123412, 1231, '2022-12-12 09:10:15', null, true, 12, 'bob', null);