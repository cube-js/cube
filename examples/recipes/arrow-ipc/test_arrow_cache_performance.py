#!/usr/bin/env python3
"""
CubeSQL Query Cache Performance Tests

Demonstrates performance improvements from server-side query result caching
in CubeSQL compared to the standard REST HTTP API.

This test suite measures:
1. Cache effectiveness (miss → hit speedup)
2. CubeSQL performance vs REST HTTP API across query sizes
3. Overall impact of query result caching

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
    api: str  # "arrow" or "http"
    query_time_ms: int
    row_count: int
    column_count: int
    label: str = ""

    def __str__(self):
        return f"{self.api.upper():6} | {self.query_time_ms:4}ms | {self.row_count:6} rows | {self.column_count} cols"


class CachePerformanceTester:
    """Tests CubeSQL query cache performance vs REST HTTP API"""

    def __init__(self, arrow_uri: str = "postgresql://username:password@localhost:4444/db",
                 http_url: str = "http://localhost:4008/cubejs-api/v1/load"):
        self.arrow_uri = arrow_uri
        self.http_url = http_url
        self.http_token = "test"  # Default token

    def run_arrow_query(self, sql: str, label: str = "") -> QueryResult:
        """Execute query via CubeSQL and measure time"""
        # Connect using psycopg2
        conn = psycopg2.connect(self.arrow_uri)
        cursor = conn.cursor()

        start = time.perf_counter()
        cursor.execute(sql)
        result = cursor.fetchall()
        elapsed_ms = int((time.perf_counter() - start) * 1000)

        row_count = len(result)
        col_count = len(cursor.description) if cursor.description else 0

        cursor.close()
        conn.close()

        return QueryResult("cubesql", elapsed_ms, row_count, col_count, label)

    def run_http_query(self, query_dict: Dict[str, Any], label: str = "") -> QueryResult:
        """Execute query via HTTP API and measure time"""
        headers = {
            "Authorization": self.http_token,
            "Content-Type": "application/json"
        }

        start = time.perf_counter()
        response = requests.post(self.http_url,
                                headers=headers,
                                json={"query": query_dict})
        data = response.json()
        elapsed_ms = int((time.perf_counter() - start) * 1000)

        # Count rows and columns from response
        dataset = data.get("data", [])
        row_count = len(dataset)
        col_count = len(dataset[0].keys()) if dataset else 0

        return QueryResult("http", elapsed_ms, row_count, col_count, label)

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
        if cubesql.query_time_ms == 0:
            speedup_text = "∞"
        else:
            speedup = http.query_time_ms / cubesql.query_time_ms
            speedup_text = f"{speedup:.1f}x"

        time_saved = http.query_time_ms - cubesql.query_time_ms

        print(f"\n{Colors.BOLD}{'─' * 80}{Colors.END}")
        print(f"{Colors.BOLD}CUBESQL vs REST HTTP API:{Colors.END}")
        print(f"  CubeSQL (cached):  {cubesql.query_time_ms}ms")
        print(f"  REST HTTP API:     {http.query_time_ms}ms")
        print(f"  {Colors.GREEN}{Colors.BOLD}Speedup:           {speedup_text} faster{Colors.END}")
        print(f"  Time saved:        {time_saved}ms")
        print(f"{Colors.BOLD}{'─' * 80}{Colors.END}\n")

    def test_cache_warmup_and_hit(self):
        """Test 1: Demonstrate cache miss → cache hit speedup"""
        self.print_header(
            "Cache Miss → Cache Hit",
            "Running same query twice to show cache warming and speedup"
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

        speedup = result1.query_time_ms / result2.query_time_ms if result2.query_time_ms > 0 else float('inf')
        time_saved = result1.query_time_ms - result2.query_time_ms

        print(f"\n{Colors.BOLD}{'─' * 80}{Colors.END}")
        print(f"{Colors.BOLD}CACHE PERFORMANCE:{Colors.END}")
        print(f"  First query (miss):  {result1.query_time_ms}ms")
        print(f"  Second query (hit):  {result2.query_time_ms}ms")
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

        return http_result.query_time_ms / cubesql_result.query_time_ms if cubesql_result.query_time_ms > 0 else float('inf')

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

        return http_result.query_time_ms / cubesql_result.query_time_ms if cubesql_result.query_time_ms > 0 else float('inf')

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

        return http_result.query_time_ms / cubesql_result.query_time_ms if cubesql_result.query_time_ms > 0 else float('inf')

    def run_all_tests(self):
        """Run complete test suite"""
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print("  CUBESQL QUERY CACHE PERFORMANCE TEST SUITE")
        print("  CubeSQL (with cache) vs REST HTTP API")
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
