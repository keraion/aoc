import sys
import duckdb

if __name__ == "__main__":
    with duckdb.connect() as ddb:
        duckdb.query(sys.stdin.read()).show()