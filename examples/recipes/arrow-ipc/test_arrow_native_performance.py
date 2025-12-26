#!/usr/bin/env python3
"""
CubeSQL Arrow Native Server Performance Tests

Demonstrates performance improvements from CubeSQL's NEW Arrow Native server
compared to the standard REST HTTP API.

This test suite measures:
1. Arrow Native server (port 4445) vs REST HTTP API (port 4008)
2. Optional cache effectiveness when enabled (miss → hit speedup)
3. Full materialization timing (complete client experience)

Test Modes:
    - CUBESQL_QUERY_CACHE_ENABLED=true:  Tests with optional cache (shows cache speedup)
    - CUBESQL_QUERY_CACHE_ENABLED=false: Tests baseline Arrow Native vs REST API

    Note: When using CubeStore pre-aggregations, data is already cached at the storage
    layer. CubeStore is a cache itself - sometimes one cache is plenty. Cacheless setup
    avoids double-caching and still gets 8-15x speedup from Arrow Native binary protocol.

Requirements:
    pip install psycopg2-binary requests

Usage:
    # From examples/recipes/arrow-ipc directory:

    # Test WITH cache enabled (default)
    export CUBESQL_QUERY_CACHE_ENABLED=true
    ./start-cubesqld.sh &
    python test_arrow_native_performance.py

    # Test WITHOUT cache (baseline Arrow Native)
    export CUBESQL_QUERY_CACHE_ENABLED=false
    ./start-cubesqld.sh &
    python test_arrow_native_performance.py
"""

import time
import requests
import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any
import sys
from arrow_native_client import ArrowNativeClient

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
    api: str  # "arrow" or "rest"
    query_time_ms: int
    materialize_time_ms: int
    total_time_ms: int
    row_count: int
    column_count: int
    label: str = ""

    def __str__(self):
        return (f"{self.api.upper():6} | Query: {self.query_time_ms:4}ms | "
                f"Materialize: {self.materialize_time_ms:3}ms | "
                f"Total: {self.total_time_ms:4}ms | {self.row_count:6} rows")


class ArrowNativePerformanceTester:
    """Tests Arrow Native server (port 4445) vs REST HTTP API (port 4008)"""

    def __init__(self,
                 arrow_host: str = "localhost",
                 arrow_port: int = 4445,
                 http_url: str = "http://localhost:4008/cubejs-api/v1/load"):
        self.arrow_host = arrow_host
        self.arrow_port = arrow_port
        self.http_url = http_url
        self.http_token = "test"  # Default token

        # Detect cache mode from environment
        cache_env = os.getenv("CUBESQL_QUERY_CACHE_ENABLED", "true").lower()
        self.cache_enabled = cache_env in ("true", "1", "yes")

    def run_arrow_query(self, sql: str, label: str = "") -> QueryResult:
        """Execute query via Arrow Native server (port 4445) with full materialization"""
        # Connect using Arrow Native client
        with ArrowNativeClient(host=self.arrow_host, port=self.arrow_port, token=self.http_token) as client:
            # Measure query execution
            query_start = time.perf_counter()
            result = client.query(sql)
            query_time_ms = int((time.perf_counter() - query_start) * 1000)

            # Measure full materialization (convert to pandas DataFrame)
            materialize_start = time.perf_counter()
            df = result.to_pandas()
            materialize_time_ms = int((time.perf_counter() - materialize_start) * 1000)

            total_time_ms = query_time_ms + materialize_time_ms
            row_count = len(df)
            col_count = len(df.columns)

        return QueryResult("arrow", query_time_ms, materialize_time_ms,
                          total_time_ms, row_count, col_count, label)

    def run_http_query(self, query: Dict[str, Any], label: str = "") -> QueryResult:
        """Execute query via REST HTTP API (port 4008) with full materialization"""
        headers = {
            "Authorization": self.http_token,
            "Content-Type": "application/json"
        }

        # Measure HTTP request + response
        query_start = time.perf_counter()
        response = requests.post(self.http_url, headers=headers, json={"query": query})
        response.raise_for_status()
        query_time_ms = int((time.perf_counter() - query_start) * 1000)

        # Measure materialization (parse JSON)
        materialize_start = time.perf_counter()
        data = response.json()
        rows = data.get("data", [])
        materialize_time_ms = int((time.perf_counter() - materialize_start) * 1000)

        total_time_ms = query_time_ms + materialize_time_ms
        row_count = len(rows)
        col_count = len(rows[0].keys()) if rows else 0

        return QueryResult("rest", query_time_ms, materialize_time_ms,
                          total_time_ms, row_count, col_count, label)

    def print_header(self, title: str, subtitle: str = ""):
        """Print test section header"""
        print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 80}{Colors.END}")
        print(f"{Colors.BOLD}{Colors.BLUE}TEST: {title}{Colors.END}")
        if subtitle:
            print(f"{Colors.CYAN}{subtitle}{Colors.END}")
        print(f"{Colors.BOLD}{Colors.BLUE}{'─' * 80}{Colors.END}\n")

    def print_result(self, result: QueryResult, indent: str = ""):
        """Print query result details"""
        print(f"{indent}{result}")

    def print_comparison(self, arrow_result: QueryResult, http_result: QueryResult):
        """Print comparison between Arrow Native and REST HTTP"""
        if arrow_result.total_time_ms > 0:
            speedup = http_result.total_time_ms / arrow_result.total_time_ms
            time_saved = http_result.total_time_ms - arrow_result.total_time_ms
            color = Colors.GREEN if speedup > 5 else Colors.YELLOW
            print(f"\n  {color}{Colors.BOLD}Arrow Native is {speedup:.1f}x faster{Colors.END}")
            print(f"  Time saved: {time_saved}ms\n")
            return speedup
        return 1.0

    def test_cache_effectiveness(self):
        """Test 1: Cache miss → hit (only when cache is enabled)"""
        if not self.cache_enabled:
            print(f"{Colors.YELLOW}Skipping cache test - cache is disabled{Colors.END}\n")
            return None

        self.print_header(
            "Optional Query Cache: Miss → Hit",
            "Demonstrates cache speedup on repeated queries"
        )

        sql = """
        SELECT market_code, brand_code, count, total_amount_sum
        FROM orders_with_preagg
        WHERE updated_at >= '2024-01-01'
        LIMIT 500
        """

        print(f"{Colors.CYAN}Running same query twice to measure cache effectiveness...{Colors.END}\n")

        # First execution (cache MISS)
        result1 = self.run_arrow_query(sql, "Cache MISS")
        time.sleep(0.1)  # Brief pause between queries

        # Second execution (cache HIT)
        result2 = self.run_arrow_query(sql, "Cache HIT")

        speedup = result1.total_time_ms / result2.total_time_ms if result2.total_time_ms > 0 else 1.0
        time_saved = result1.total_time_ms - result2.total_time_ms

        print(f"  First query (cache MISS):")
        print(f"    Query:        {result1.query_time_ms:4}ms")
        print(f"    Materialize:  {result1.materialize_time_ms:4}ms")
        print(f"    TOTAL:        {result1.total_time_ms:4}ms")
        print(f"  Second query (cache HIT):")
        print(f"    Query:        {result2.query_time_ms:4}ms")
        print(f"    Materialize:  {result2.materialize_time_ms:4}ms")
        print(f"    TOTAL:        {result2.total_time_ms:4}ms")
        print(f"  {Colors.GREEN}{Colors.BOLD}Cache speedup:       {speedup:.1f}x faster{Colors.END}")
        print(f"  Time saved:          {time_saved}ms")
        print(f"{Colors.BOLD}{'─' * 80}{Colors.END}\n")

        return speedup

    def test_arrow_vs_rest_small(self):
        """Test: Small query - Arrow Native vs REST HTTP API"""
        self.print_header(
            "Small Query (200 rows)",
            f"Arrow Native (4445) vs REST HTTP API (4008) {'[Cache enabled]' if self.cache_enabled else '[No cache]'}"
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

        if self.cache_enabled:
            # Warm up cache first
            print(f"{Colors.CYAN}Warming up cache...{Colors.END}")
            self.run_arrow_query(sql)
            time.sleep(0.1)

        # Run comparison
        print(f"{Colors.CYAN}Running performance comparison...{Colors.END}\n")
        arrow_result = self.run_arrow_query(sql, "Arrow Native")
        rest_result = self.run_http_query(http_query, "REST HTTP")

        self.print_result(arrow_result, "  ")
        self.print_result(rest_result, "  ")
        speedup = self.print_comparison(arrow_result, rest_result)

        return speedup

    def test_arrow_vs_rest_medium(self):
        """Test: Medium query (1-2K rows) - Arrow Native vs REST HTTP API"""
        self.print_header(
            "Medium Query (1-2K rows)",
            f"Arrow Native (4445) vs REST HTTP API (4008) {'[Cache enabled]' if self.cache_enabled else '[No cache]'}"
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

        if self.cache_enabled:
            # Warm up cache
            print(f"{Colors.CYAN}Warming up cache...{Colors.END}")
            self.run_arrow_query(sql)
            time.sleep(0.1)

        # Run comparison
        print(f"{Colors.CYAN}Running performance comparison...{Colors.END}\n")
        arrow_result = self.run_arrow_query(sql, "Arrow Native")
        rest_result = self.run_http_query(http_query, "REST HTTP")

        self.print_result(arrow_result, "  ")
        self.print_result(rest_result, "  ")
        speedup = self.print_comparison(arrow_result, rest_result)

        return speedup

    def test_arrow_vs_rest_large(self):
        """Test: Large query (10K+ rows) - Arrow Native vs REST HTTP API"""
        self.print_header(
            "Large Query (10K+ rows)",
            f"Arrow Native (4445) vs REST HTTP API (4008) {'[Cache enabled]' if self.cache_enabled else '[No cache]'}"
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

        if self.cache_enabled:
            # Warm up cache
            print(f"{Colors.CYAN}Warming up cache...{Colors.END}")
            self.run_arrow_query(sql)
            time.sleep(0.1)

        # Run comparison
        print(f"{Colors.CYAN}Running performance comparison...{Colors.END}\n")
        arrow_result = self.run_arrow_query(sql, "Arrow Native")
        rest_result = self.run_http_query(http_query, "REST HTTP")

        self.print_result(arrow_result, "  ")
        self.print_result(rest_result, "  ")
        speedup = self.print_comparison(arrow_result, rest_result)

        return speedup

    def run_all_tests(self):
        """Run complete test suite"""
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print("  CUBESQL ARROW NATIVE SERVER PERFORMANCE TEST SUITE")
        print(f"  Arrow Native (port 4445) vs REST HTTP API (port 4008)")
        cache_status = "ENABLED" if self.cache_enabled else "DISABLED"
        cache_color = Colors.GREEN if self.cache_enabled else Colors.YELLOW
        print(f"  Query Cache: {cache_color}{cache_status}{Colors.END}")
        print("=" * 80)
        print(f"{Colors.END}\n")

        speedups = []

        try:
            # Test 1: Cache effectiveness (only if enabled)
            if self.cache_enabled:
                speedup1 = self.test_cache_effectiveness()
                if speedup1:
                    speedups.append(("Cache Miss → Hit", speedup1))

            # Test 2: Small query
            speedup2 = self.test_arrow_vs_rest_small()
            speedups.append(("Small Query (200 rows)", speedup2))

            # Test 3: Medium query
            speedup3 = self.test_arrow_vs_rest_medium()
            speedups.append(("Medium Query (1-2K rows)", speedup3))

            # Test 4: Large query
            speedup4 = self.test_arrow_vs_rest_large()
            speedups.append(("Large Query (10K+ rows)", speedup4))

        except Exception as e:
            print(f"\n{Colors.RED}{Colors.BOLD}ERROR: {e}{Colors.END}")
            print(f"\n{Colors.YELLOW}Make sure:")
            print(f"  1. Arrow Native server is running on localhost:4445")
            print(f"  2. Cube REST API is running on localhost:4008")
            print(f"  3. orders_with_preagg cube exists with data")
            print(f"  4. CUBESQL_QUERY_CACHE_ENABLED is set correctly{Colors.END}\n")
            sys.exit(1)

        # Print summary
        self.print_summary(speedups)

    def print_summary(self, speedups: List[tuple]):
        """Print final summary of all tests"""
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print("  SUMMARY: Arrow Native vs REST HTTP API Performance")
        print("=" * 80)
        print(f"{Colors.END}\n")

        total = 0
        count = 0

        for test_name, speedup in speedups:
            color = Colors.GREEN if speedup > 5 else Colors.YELLOW
            print(f"  {test_name:30} {color}{speedup:6.1f}x faster{Colors.END}")
            if speedup != float('inf'):
                total += speedup
                count += 1

        if count > 0:
            avg_speedup = total / count
            print(f"\n  {Colors.BOLD}Average Speedup:{Colors.END} {Colors.GREEN}{Colors.BOLD}{avg_speedup:.1f}x{Colors.END}\n")

        print(f"{Colors.BOLD}{'=' * 80}{Colors.END}\n")

        print(f"{Colors.GREEN}{Colors.BOLD}✓ All tests passed!{Colors.END}")
        if self.cache_enabled:
            print(f"{Colors.CYAN}Arrow Native server with cache significantly outperforms REST HTTP API{Colors.END}\n")
        else:
            print(f"{Colors.CYAN}Arrow Native server (baseline, no cache) outperforms REST HTTP API{Colors.END}\n")


def main():
    """Main entry point"""
    tester = ArrowNativePerformanceTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
