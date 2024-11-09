
```bash
docker compose run app bash
```


```bash
cargo pgrx run
```


```sql
create extension pg_elephantduck;
pg_elephantduck=# select * from pg_am;
  oid  |    amname    |        amhandler        | amtype 
-------+--------------+-------------------------+--------
 16406 | elephantduck | pg_elephantduck_handler | t
(8 rows)
```

