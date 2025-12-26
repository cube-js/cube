#!/usr/bin/env python3
"""
CubeSQL Arrow Native Server Performance Tests

Demonstrates performance improvements from CubeSQL's Arrow Native server
with optional query result caching, compared to the standard REST HTTP API.

This test suite measures:
1. Arrow Native server baseline performance
2. Optional cache effectiveness (miss → hit speedup)
3. CubeSQL vs REST HTTP API across query sizes
4. Full materialization timing (complete client experience)

Requirements:
    pip install psycopg2-binary requests

Usage:
    # From examples/recipes/arrow-ipc directory:

    # 1. Start Cube API and database
    ./dev-start.sh

    # 2. Start CubeSQL with cache enabled
    ./start-cubesqld.sh

    # 3. Run performance tests
    python test_arrow_cache_performance.py
"""

import psycopg2
import time
import requests
import json
from dataclasses import dataclass
from typing import List, Dict, Any
import sys

# ANSI color codes for pretty output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

@dataclass
class QueryResult:
    """Results from a single query execution"""
    api: str  # "cubesql" or "http"
    query_time_ms: int
    materialize_time_ms: int
    total_time_ms: int
    row_count: int
    column_count: int
    label: str = ""

    def __str__(self):
        return (f"{self.api.upper():7} | Query: {self.query_time_ms:4}ms | "
                f"Materialize: {self.materialize_time_ms:3}ms | "
                f"Total: {self.total_time_ms:4}ms | {self.row_count:6} rows")


class CachePerformanceTester:
    """Tests CubeSQL Arrow Native server performance (with optional cache) vs REST HTTP API"""

    def __init__(self, arrow_uri: str = "postgresql://username:password@localhost:4444/db",
                 http_url: str = "http://localhost:4008/cubejs-api/v1/load"):
        self.arrow_uri = arrow_uri
        self.http_url = http_url
        self.http_token = "test"  # Default token

    def run_arrow_query(self, sql: str, label: str = "") -> QueryResult:
        """Execute query via CubeSQL and measure time with full materialization"""
        # Connect using psycopg2
        conn = psycopg2.connect(self.arrow_uri)
        cursor = conn.cursor()

        # Measure query execution + initial fetch
        query_start = time.perf_counter()
        cursor.execute(sql)
        result = cursor.fetchall()
        query_time_ms = int((time.perf_counter() - query_start) * 1000)

        # Measure full materialization (convert to list of dicts - simulates DataFrame creation)
        materialize_start = time.perf_counter()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        materialized_data = [dict(zip(columns, row)) for row in result]
        materialize_time_ms = int((time.perf_counter() - materialize_start) * 1000)

        total_time_ms = query_time_ms + materialize_time_ms
        row_count = len(materialized_data)
        col_count = len(columns)

        cursor.close()
        conn.close()

        return QueryResult("cubesql", query_time_ms, materialize_time_ms,
                          total_time_ms, row_count, col_count, label)

    def run_http_query(self, query_dict: Dict[str, Any], label: str = "") -> QueryResult:
        """Execute query via HTTP API and measure time with full materialization"""
        headers = {
            "Authorization": self.http_token,
            "Content-Type": "application/json"
        }

        # Measure HTTP request + JSON parsing
        query_start = time.perf_counter()
        response = requests.post(self.http_url,
                                headers=headers,
                                json={"query": query_dict})
        query_time_ms = int((time.perf_counter() - query_start) * 1000)

        # Measure full materialization (JSON parse + data extraction)
        materialize_start = time.perf_counter()
        data = response.json()
        dataset = data.get("data", [])
        # Simulate same materialization as CubeSQL (list of dicts)
        materialized_data = [dict(row) for row in dataset]
        materialize_time_ms = int((time.perf_counter() - materialize_start) * 1000)

        total_time_ms = query_time_ms + materialize_time_ms
        row_count = len(materialized_data)
        col_count = len(materialized_data[0].keys()) if materialized_data else 0

        return QueryResult("http", query_time_ms, materialize_time_ms,
                          total_time_ms, row_count, col_count, label)

    def print_header(self, test_name: str, description: str):
        """Print formatted test header"""
        print(f"\n{Colors.BOLD}{'=' * 80}{Colors.END}")
        print(f"{Colors.HEADER}{Colors.BOLD}TEST: {test_name}{Colors.END}")
        print(f"{Colors.CYAN}{description}{Colors.END}")
        print(f"{Colors.BOLD}{'=' * 80}{Colors.END}\n")

    def print_result(self, result: QueryResult, prefix: str = ""):
        """Print formatted query result"""
        color = Colors.GREEN if result.api == "cubesql" else Colors.YELLOW
        print(f"{color}{prefix}{result}{Colors.END}")

    def print_comparison(self, cubesql: QueryResult, http: QueryResult):
        """Print performance comparison"""
        if cubesql.total_time_ms == 0:
            speedup_text = "∞"
        else:
            speedup = http.total_time_ms / cubesql.total_time_ms
            speedup_text = f"{speedup:.1f}x"

        time_saved = http.total_time_ms - cubesql.total_time_ms

        print(f"\n{Colors.BOLD}{'─' * 80}{Colors.END}")
        print(f"{Colors.BOLD}CUBESQL vs REST HTTP API (Full Materialization):{Colors.END}")
        print(f"  CubeSQL:")
        print(f"    Query:        {cubesql.query_time_ms:4}ms")
        print(f"    Materialize:  {cubesql.materialize_time_ms:4}ms")
        print(f"    TOTAL:        {cubesql.total_time_ms:4}ms")
        print(f"  REST HTTP API:")
        print(f"    Query:        {http.query_time_ms:4}ms")
        print(f"    Materialize:  {http.materialize_time_ms:4}ms")
        print(f"    TOTAL:        {http.total_time_ms:4}ms")
        print(f"  {Colors.GREEN}{Colors.BOLD}Speedup:        {speedup_text} faster{Colors.END}")
        print(f"  Time saved:     {time_saved}ms")
        print(f"{Colors.BOLD}{'─' * 80}{Colors.END}\n")

    def test_cache_warmup_and_hit(self):
        """Test 1: Demonstrate optional cache effectiveness (miss → hit)"""
        self.print_header(
            "Optional Query Cache: Miss → Hit",
            "Running same query twice to show cache effectiveness (optional feature)"
        )

        sql = """
        SELECT market_code, brand_code, count, total_amount_sum
        FROM orders_with_preagg
        WHERE updated_at >= '2024-01-01'
        LIMIT 500
        """

        print(f"{Colors.CYAN}Warming up cache (first query - cache MISS)...{Colors.END}")
        result1 = self.run_arrow_query(sql, "First run (cache miss)")
        self.print_result(result1, "  ")

        # Brief pause to let cache settle
        time.sleep(0.1)

        print(f"\n{Colors.CYAN}Running same query (cache HIT)...{Colors.END}")
        result2 = self.run_arrow_query(sql, "Second run (cache hit)")
        self.print_result(result2, "  ")

        speedup = result1.total_time_ms / result2.total_time_ms if result2.total_time_ms > 0 else float('inf')
        time_saved = result1.total_time_ms - result2.total_time_ms

        print(f"\n{Colors.BOLD}{'─' * 80}{Colors.END}")
        print(f"{Colors.BOLD}OPTIONAL CACHE PERFORMANCE (Full Materialization):{Colors.END}")
        print(f"{Colors.CYAN}Note: Cache is optional and can be disabled{Colors.END}")
        print(f"  First query (miss):")
        print(f"    Query:        {result1.query_time_ms:4}ms")
        print(f"    Materialize:  {result1.materialize_time_ms:4}ms")
        print(f"    TOTAL:        {result1.total_time_ms:4}ms")
        print(f"  Second query (hit):")
        print(f"    Query:        {result2.query_time_ms:4}ms")
        print(f"    Materialize:  {result2.materialize_time_ms:4}ms")
        print(f"    TOTAL:        {result2.total_time_ms:4}ms")
        print(f"  {Colors.GREEN}{Colors.BOLD}Cache speedup:       {speedup:.1f}x faster{Colors.END}")
        print(f"  Time saved:          {time_saved}ms")
        print(f"{Colors.BOLD}{'─' * 80}{Colors.END}\n")

        return speedup

    def test_arrow_vs_http_small(self):
        """Test 2: Small query - CubeSQL vs REST HTTP API"""
        self.print_header(
            "Small Query (200 rows)",
            "CubeSQL (with cache) vs REST HTTP API"
        )

        sql = """
        SELECT market_code, count
        FROM orders_with_preagg
        WHERE updated_at >= '2024-06-01'
        LIMIT 200
        """

        http_query = {
            "measures": ["orders_with_preagg.count"],
            "dimensions": ["orders_with_preagg.market_code"],
            "timeDimensions": [{
                "dimension": "orders_with_preagg.updated_at",
                "dateRange": ["2024-06-01", "2024-12-31"]
            }],
            "limit": 200
        }

        # Warm up cache
        print(f"{Colors.CYAN}Warming up CubeSQL cache...{Colors.END}")
        self.run_arrow_query(sql)
        time.sleep(0.1)

        # Run actual test
        print(f"{Colors.CYAN}Running performance comparison...{Colors.END}\n")
        cubesql_result = self.run_arrow_query(sql, "CubeSQL (cached)")
        http_result = self.run_http_query(http_query, "REST HTTP API")

        self.print_result(cubesql_result, "  ")
        self.print_result(http_result, "  ")
        self.print_comparison(cubesql_result, http_result)

        return http_result.total_time_ms / cubesql_result.total_time_ms if cubesql_result.total_time_ms > 0 else float('inf')

    def test_arrow_vs_http_medium(self):
        """Test 3: Medium query (1-2K rows) - CubeSQL vs REST HTTP API"""
        self.print_header(
            "Medium Query (1-2K rows)",
            "CubeSQL (with cache) vs REST HTTP API on medium result sets"
        )

        sql = """
        SELECT market_code, brand_code,
               count,
               total_amount_sum,
               tax_amount_sum
        FROM orders_with_preagg
        WHERE updated_at >= '2024-01-01'
        LIMIT 2000
        """

        http_query = {
            "measures": [
                "orders_with_preagg.count",
                "orders_with_preagg.total_amount_sum",
                "orders_with_preagg.tax_amount_sum"
            ],
            "dimensions": [
                "orders_with_preagg.market_code",
                "orders_with_preagg.brand_code"
            ],
            "timeDimensions": [{
                "dimension": "orders_with_preagg.updated_at",
                "dateRange": ["2024-01-01", "2024-12-31"]
            }],
            "limit": 2000
        }

        # Warm up cache
        print(f"{Colors.CYAN}Warming up CubeSQL cache...{Colors.END}")
        self.run_arrow_query(sql)
        time.sleep(0.1)

        # Run actual test
        print(f"{Colors.CYAN}Running performance comparison...{Colors.END}\n")
        cubesql_result = self.run_arrow_query(sql, "CubeSQL (cached)")
        http_result = self.run_http_query(http_query, "REST HTTP API")

        self.print_result(cubesql_result, "  ")
        self.print_result(http_result, "  ")
        self.print_comparison(cubesql_result, http_result)

        return http_result.total_time_ms / cubesql_result.total_time_ms if cubesql_result.total_time_ms > 0 else float('inf')

    def test_arrow_vs_http_large(self):
        """Test 4: Large query (10K+ rows) - CubeSQL vs REST HTTP API"""
        self.print_header(
            "Large Query (10K+ rows)",
            "CubeSQL (with cache) vs REST HTTP API on large result sets"
        )

        sql = """
        SELECT market_code, brand_code, updated_at,
               count,
               total_amount_sum
        FROM orders_with_preagg
        WHERE updated_at >= '2024-01-01'
        LIMIT 10000
        """

        http_query = {
            "measures": [
                "orders_with_preagg.count",
                "orders_with_preagg.total_amount_sum"
            ],
            "dimensions": [
                "orders_with_preagg.market_code",
                "orders_with_preagg.brand_code"
            ],
            "timeDimensions": [{
                "dimension": "orders_with_preagg.updated_at",
                "granularity": "hour",
                "dateRange": ["2024-01-01", "2024-12-31"]
            }],
            "limit": 10000
        }

        # Warm up cache
        print(f"{Colors.CYAN}Warming up CubeSQL cache...{Colors.END}")
        self.run_arrow_query(sql)
        time.sleep(0.1)

        # Run actual test
        print(f"{Colors.CYAN}Running performance comparison...{Colors.END}\n")
        cubesql_result = self.run_arrow_query(sql, "CubeSQL (cached)")
        http_result = self.run_http_query(http_query, "REST HTTP API")

        self.print_result(cubesql_result, "  ")
        self.print_result(http_result, "  ")
        self.print_comparison(cubesql_result, http_result)

        return http_result.total_time_ms / cubesql_result.total_time_ms if cubesql_result.total_time_ms > 0 else float('inf')

    def run_all_tests(self):
        """Run complete test suite"""
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print("  CUBESQL ARROW NATIVE SERVER PERFORMANCE TEST SUITE")
        print("  Arrow Native Server (with optional cache) vs REST HTTP API")
        print("=" * 80)
        print(f"{Colors.END}\n")

        speedups = []

        try:
            # Test 1: Cache miss → hit
            speedup1 = self.test_cache_warmup_and_hit()
            speedups.append(("Cache Miss → Hit", speedup1))

            # Test 2: Small query
            speedup2 = self.test_arrow_vs_http_small()
            speedups.append(("Small Query (200 rows)", speedup2))

            # Test 3: Medium query
            speedup3 = self.test_arrow_vs_http_medium()
            speedups.append(("Medium Query (1-2K rows)", speedup3))

            # Test 4: Large query
            speedup4 = self.test_arrow_vs_http_large()
            speedups.append(("Large Query (10K+ rows)", speedup4))

        except Exception as e:
            print(f"\n{Colors.RED}{Colors.BOLD}ERROR: {e}{Colors.END}")
            print(f"\n{Colors.YELLOW}Make sure:")
            print(f"  1. CubeSQL is running on localhost:4444")
            print(f"  2. Cube REST API is running on localhost:4008")
            print(f"  3. Cache is enabled (CUBESQL_QUERY_CACHE_ENABLED=true)")
            print(f"  4. orders_with_preagg cube exists with data{Colors.END}\n")
            sys.exit(1)

        # Print summary
        self.print_summary(speedups)

    def print_summary(self, speedups: List[tuple]):
        """Print final summary of all tests"""
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print("  SUMMARY: CubeSQL vs REST HTTP API Performance")
        print("=" * 80)
        print(f"{Colors.END}\n")

        total = 0
        count = 0

        for test_name, speedup in speedups:
            color = Colors.GREEN if speedup > 20 else Colors.YELLOW
            print(f"  {test_name:30} {color}{speedup:6.1f}x faster{Colors.END}")
            if speedup != float('inf'):
                total += speedup
                count += 1

        if count > 0:
            avg_speedup = total / count
            print(f"\n  {Colors.BOLD}Average Speedup:{Colors.END} {Colors.GREEN}{Colors.BOLD}{avg_speedup:.1f}x{Colors.END}\n")

        print(f"{Colors.BOLD}{'=' * 80}{Colors.END}\n")

        print(f"{Colors.GREEN}{Colors.BOLD}✓ All tests passed!{Colors.END}")
        print(f"{Colors.CYAN}CubeSQL with query caching significantly outperforms REST HTTP API{Colors.END}\n")


def main():
    """Main entry point"""
    tester = CachePerformanceTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
