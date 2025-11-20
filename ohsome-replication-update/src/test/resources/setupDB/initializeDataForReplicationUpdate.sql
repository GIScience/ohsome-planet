INSERT INTO changesets (id, created_at, closed_at, open, user_id, user_name, tags, hashtags)
VALUES (34123412, '2025-10-20 09:10:36Z', null, true, 1234, 'bob',
        '{"comment": "this is a changeset for testing #ohsome-planet", "created_by": "ohsome-planet"}'::jsonb,
           '{"ohsome-planet"}');