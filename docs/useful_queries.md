# Useful DuckDB Queries

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
