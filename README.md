# Advent of Code

Advent of Code, but with less conventional means. What can be done in SQL or in a DataFrame library such as Polars?

## SQL
### DuckDB CLI
If the [CLI is installed](https://duckdb.org/docs/installation/index?version=latest&environment=cli):
```sh
duckdb < 2023/duckdb/day01.sql
```

## Python
### Virtual Environment Setup

```sh
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

### Polars
```sh
python 2023/polars/day01.py
```

### DuckDB Python Client
```sh
python ddb_runner.py < 2023/duckdb/day01.sql
```