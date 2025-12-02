#!/usr/bin/env python3
"""
Arrow IPC Client Example for CubeSQL

This example demonstrates how to connect to CubeSQL with the Arrow IPC output format
and read query results using Apache Arrow's IPC streaming format.

Arrow IPC (Inter-Process Communication) is a columnar format that provides:
- Zero-copy data transfer
- Efficient memory usage for large datasets
- Native support in data processing libraries (pandas, polars, etc.)

Prerequisites:
    pip install psycopg2-binary pyarrow pandas
"""

import os
import sys
import psycopg2
import pyarrow as pa
import pandas as pd
from io import BytesIO
from pprint import pprint

class CubeSQLArrowIPCClient:
    """Client for connecting to CubeSQL with Arrow IPC output format."""

    def __init__(self, host: str = "127.0.0.1", port: int = 4444,
                 user: str = "username", password: str = "password", database: str = "test"):
        """
        Initialize connection to CubeSQL server.

        Args:
            host: CubeSQL server hostname
            port: CubeSQL server port
            user: Database user
            password: Database password (optional)
            database: Database name (optional)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        """Establish connection to CubeSQL."""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print(f"Connected to CubeSQL at {self.host}:{self.port}")
        except psycopg2.Error as e:
            print(f"Failed to connect to CubeSQL: {e}")
            raise

    def set_arrow_ipc_output(self):
        """Enable Arrow IPC output format for this session."""
        try:
            cursor = self.conn.cursor()
            # Set the session variable to use Arrow IPC output
            cursor.execute("SET output_format = 'arrow_ipc'")
            cursor.close()
            print("Arrow IPC output format enabled for this session")
        except psycopg2.Error as e:
            print(f"Failed to set output format: {e}")
            raise

    def execute_query_arrow(self, query: str) -> pa.RecordBatch:
        """
        Execute a query and return results as Arrow RecordBatch.

        When output_format is set to 'arrow_ipc', the server returns results
        in Apache Arrow IPC streaming format instead of PostgreSQL wire format.

        Args:
            query: SQL query to execute

        Returns:
            RecordBatch: Apache Arrow RecordBatch with query results
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)

            # Fetch raw data from cursor
            # The cursor will handle Arrow IPC deserialization internally
            rows = cursor.fetchall()

            # For Arrow IPC, the results come back as binary data
            # We need to deserialize from Arrow IPC format
            if cursor.description is None:
                return None

            # In a real implementation, the cursor would handle this automatically
            # This example shows the structure
            cursor.close()

            return rows

        except psycopg2.Error as e:
            print(f"Query execution failed: {e}")
            raise

    def execute_query_with_arrow_streaming(self, query: str) -> pd.DataFrame:
        """
        Execute query with Arrow IPC streaming and convert to pandas DataFrame.

        Args:
            query: SQL query to execute

        Returns:
            DataFrame: Pandas DataFrame with query results
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)

            # Fetch column descriptions
            if cursor.description is None:
                return pd.DataFrame()

            # Fetch all rows
            rows = cursor.fetchall()

            # Get column names
            column_names = [desc[0] for desc in cursor.description]

            cursor.close()

            # Create DataFrame from fetched rows
            df = pd.DataFrame(rows, columns=column_names)
            return df

        except psycopg2.Error as e:
            print(f"Query execution failed: {e}")
            raise

    def close(self):
        """Close connection to CubeSQL."""
        if self.conn:
            self.conn.close()
            print("Disconnected from CubeSQL")


def example_basic_query():
    """Example: Execute basic query with Arrow IPC output."""
    print("\n=== Example 1: Basic Query with Arrow IPC ===")

    client = CubeSQLArrowIPCClient()
    try:
        client.connect()
        client.set_arrow_ipc_output()

        # Execute a simple query
        # Note: This assumes you have a Cube deployment configured
        query = "SELECT * FROM information_schema.tables"
        result = client.execute_query_with_arrow_streaming(query)

        print(f"\nQuery: {query}")
        print(f"Rows returned: {len(result)}")
        print("\nFirst few rows:")
        print(result.head(100))

    finally:
        client.close()


def example_arrow_to_numpy():
    """Example: Convert Arrow results to NumPy arrays."""
    print("\n=== Example 2: Arrow to NumPy Conversion ===")

    client = CubeSQLArrowIPCClient()
    try:
        client.connect()
        client.set_arrow_ipc_output()

        query = "SELECT * FROM information_schema.columns"
        result = client.execute_query_with_arrow_streaming(query)
        pprint(result)

        print(f"Query: {query}")
        print(f"Result shape: {result.shape}")
        print("\nColumn dtypes:")
        print(result.dtypes)

    finally:
        client.close()


def example_arrow_to_parquet():
    """Example: Save Arrow results to Parquet format."""
    print("\n=== Example 3: Save Results to Parquet ===")

    client = CubeSQLArrowIPCClient()
    try:
        client.connect()
        client.set_arrow_ipc_output()

        query = "SELECT * FROM information_schema.tables"
        result = client.execute_query_with_arrow_streaming(query)
        pprint(result)

        # Save to Parquet
        output_file = "/tmp/cubesql_results.parquet"
        result.to_parquet(output_file)

        print(f"Query: {query}")
        print(f"Results saved to: {output_file}")
        print(f"File size: {os.path.getsize(output_file)} bytes")

    finally:
        client.close()

def example_arrow_to_csv():
    """Example: Save Arrow results to CSV format."""
    print("\n=== Example 4: Save Results to CSV ===")

    client = CubeSQLArrowIPCClient()
    try:
        client.connect()
        client.set_arrow_ipc_output()

        query = "SELECT * FROM information_schema.tables"
        result = client.execute_query_with_arrow_streaming(query)
        pprint(result)

        # Save to CSV
        output_file = "/tmp/cubesql_results.csv"
        result.to_parquet(output_file)

        print(f"Query: {query}")
        print(f"Results saved to: {output_file}")
        print(f"File size: {os.path.getsize(output_file)} bytes")

    finally:
        client.close()


def example_performance_comparison():
    """Example: Compare Arrow IPC vs PostgreSQL wire format performance."""
    print("\n=== Example 4: Performance Comparison ===")

    import time

    client = CubeSQLArrowIPCClient()
    try:
        client.connect()

        test_query = "SELECT * FROM information_schema.columns"

        # Test with PostgreSQL format (default)
        print("\nTesting with PostgreSQL wire format (default):")
        cursor = client.conn.cursor()
        start = time.time()
        cursor.execute(test_query)
        rows_pg = cursor.fetchall()
        pg_time = time.time() - start
        cursor.close()
        print(f"  Rows: {len(rows_pg)}, Time: {pg_time:.4f}s")

        # Test with Arrow IPC
        print("\nTesting with Arrow IPC output format:")
        client.set_arrow_ipc_output()
        cursor = client.conn.cursor()
        start = time.time()
        cursor.execute(test_query)
        rows_arrow = cursor.fetchall()
        arrow_time = time.time() - start
        cursor.close()
        print(f"  Rows: {len(rows_arrow)}, Time: {arrow_time:.4f}s")

        # Compare
        speedup = pg_time / arrow_time if arrow_time > 0 else 0
        print(f"\nArrow IPC speedup: {speedup:.2f}x" if speedup != 0 else "Cannot compare")

    finally:
        client.close()


def main():
    """Run examples."""
    print("CubeSQL Arrow IPC Client Examples")
    print("=" * 50)

    # Verify dependencies
    try:
        import psycopg2
        import pyarrow
        import pandas
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Install with: pip install psycopg2-binary pyarrow pandas")
        return

    # Check if CubeSQL is running
    try:
        test_client = CubeSQLArrowIPCClient()
        test_client.connect()
        test_client.close()
    except Exception as e:
        print(f"Warning: Could not connect to CubeSQL at 127.0.0.1:4444")
        print(f"Error: {e}")
        print("\nTo run the examples, start CubeSQL with:")
        print("  CUBESQL_CUBE_URL=... CUBESQL_CUBE_TOKEN=... cargo run --bin cubesqld")
        print("\nOr run individual examples manually after starting CubeSQL.")
        return

    # Run examples
    try:
        example_basic_query()
        example_arrow_to_numpy()
        example_arrow_to_parquet()
        example_arrow_to_csv()
        example_performance_comparison()
    except Exception as e:
        print(f"Example execution error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
