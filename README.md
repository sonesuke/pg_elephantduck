# pg_elephantduck: Columnar table access method for PostgreSQL powered by DuckDB

pg_elephantduck is a PostgreSQL extension that provides a columnar table access method for PostgreSQL.
This extension used DuckDB as a storage engine to store the columnar data,
and it provides a way to access the columnar data from PostgreSQL.

## Features

- Columnar table access method for PostgreSQL
- Columnar data storage using DuckDB
- Data storing as Parquet format
- Some `WHERE` clause pushdown to DuckDB

## Getting Started

This extension must be compiled from source code.

Before building and installing pg_elephantduck, one should ensure to have the following:

- PostgreSQL 16 or later (Ensure `pg_config` is in your `PATH`)
- Rust 1.82 or later

Typical installation procedure may look like this:

```bash
$ git clone https://github.com/sonesuke/pg_elephantduck.git
$ cd pg_elephantduck
$ cargo install cargo-pgrx@0.12.8 && cargo pgrx init --jobs 4 --pg16 download
$ cargo pgrx install
```

where `--jobs` specifies the number of parallel jobs to use for building the extension,
and `--pg16` specifies the PostgreSQL version to build against (It should be same as your PostgreSQL version).

After installation, you can create the extension in your database:

```sql
CREATE EXTENSION pg_elephantduck;
```

### CREATE TABLE and SELECT

```sql
CREATE TABLE sample USING elephantduck AS SELECT GENERATE_SERIES(1, 10000) AS number;
SELECT number FROM sample WHERE number < 10;
```

See [test code](./src/tests) for more examples.

## Parameters

Parquet files are stored in the directory specified by `elephantduck.path`.
`elephantduck.path` can be set in `postgresql.conf` or `SET` command and its default is `$PG_DATA/elephantduck` or `/tmp/elephantduck`.

```sql
SET elephantduck.path = '/path/to/elephantduck';
```

## Limitations

Many limitations are as bug reports. See [issues](https://github.com/sonesuke/pg_elephantduck/issues) for limitations.

## Development

After installation, you can run tests and run the extension in the development environment.

For running tests:

```bash
cargo pgrx test
```

For running the extension:

```bash
cargo pgrx run
```

If you want to develop this extension in Docker, you can use the following command:

```bash
docker compose run development
```
