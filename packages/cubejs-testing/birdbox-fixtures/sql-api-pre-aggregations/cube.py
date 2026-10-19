# Configuration options: https://docs.cube.dev/reference/configuration/config

from cube import config

@config('driver_factory')
def driver_factory(ctx: dict) -> dict:
    # Init SQL to create tables from CSV files on S3
    init_sql = """
        -- Create products table from CSV on S3
        CREATE TABLE IF NOT EXISTS products AS
        SELECT *
        FROM read_csv_auto('s3://cube-tutorial/products.csv');

        -- Create users table from CSV on S3
        CREATE TABLE IF NOT EXISTS users AS
        SELECT *
        FROM read_csv_auto('s3://cube-tutorial/users.csv');

        -- Create orders table from CSV on S3
        CREATE TABLE IF NOT EXISTS orders AS
        SELECT *
        FROM read_csv_auto('s3://cube-tutorial/orders.csv');

        -- Create line_items table from CSV on S3
        CREATE TABLE IF NOT EXISTS line_items AS
        SELECT *
        FROM read_csv_auto('s3://cube-tutorial/line_items.csv');
    """

    return {
        'type': 'duckdb',
        'initSql': init_sql,
    }
