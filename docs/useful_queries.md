# Useful DuckDB Queries

## OSM at specific snapshot timestamp

Here we extract all OSM elements as they were visible on OSM as of `2020-01-01`.

```sql
SELECT
  osm_type,
  osm_id,
  osm_version,
  tags,
  geometry
FROM read_parquet('contributions/**/*.parquet')  -- include latest and history
WHERE
  valid_from <= '2020-01-01'
  AND valid_to > '2020-01-01'
;
```

## Extract relation member geometries
```
COPY (
	select
		osm_id,
		unnest(list_transform(members, m-> struct_pack(m_id := format('{}/{}',m.type, m.id), role := m.role, geometry:= st_geomfromwkb(m.geometry))), recursive := true)
	from read_parquet('/data/out-berlin/contributions/latest/*.parquet')
	where 1=1
	    and osm_type = 'relation'
	    and map_contains_entry(tags, 'route', 'bicycle')
) to '/data/routes.parquet'
```
