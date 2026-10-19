# Configuration options: https://docs.cube.dev/reference/configuration/config

from cube import config

@config('driver_factory')
def driver_factory(ctx: dict) -> dict:
    # Init SQL to create tables from CSV files: the tutorial's s3://cube-tutorial ones,
    # vendored next to the fixture so the test needs no network
    init_sql = """
        -- Create products table from CSV
        CREATE TABLE IF NOT EXISTS products AS
        SELECT *
        FROM read_csv_auto('../birdbox-fixtures/sql-api-pre-aggregations/data/products.csv.gz');

        -- Create users table from CSV
        CREATE TABLE IF NOT EXISTS users AS
        SELECT *
        FROM read_csv_auto('../birdbox-fixtures/sql-api-pre-aggregations/data/users.csv.gz');

        -- Create orders table from CSV
        CREATE TABLE IF NOT EXISTS orders AS
        SELECT *
        FROM read_csv_auto('../birdbox-fixtures/sql-api-pre-aggregations/data/orders.csv.gz');

        -- Create line_items table from CSV
        CREATE TABLE IF NOT EXISTS line_items AS
        SELECT *
        FROM read_csv_auto('../birdbox-fixtures/sql-api-pre-aggregations/data/line_items.csv.gz');
    """

    return {
        'type': 'duckdb',
        'initSql': init_sql,
    }
