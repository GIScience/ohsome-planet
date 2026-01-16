INSERT INTO changeset_state
VALUES (10020, '2021-12-12 09:10:15');

INSERT INTO changesets (id, user_id, created_at, closed_at, open, user_name, geom, tags, hashtags)
VALUES (111, 1231, '2021-12-12 09:10:15', null, true, 'bob', null, '{}', '{}'),
       (1231, 1231, '2022-12-12 09:10:15', null, true, 'bob', null, '{}', '{}'),
       (34123412, 1231, '2022-12-12 09:10:15', null, true, 'bob', null, '{}', '{}');