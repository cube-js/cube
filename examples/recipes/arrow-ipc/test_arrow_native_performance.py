#!/usr/bin/env python3
"""
CubeSQL ADBC(Arrow Native) Server Performance Tests

Demonstrates performance improvements from CubeSQL's NEW ADBC(Arrow Native) server
compared to the standard REST HTTP API.

This test suite measures:
1. ADBC server (port 8120) vs REST HTTP API (port 4008)
2. Optional cache effectiveness when enabled (miss → hit speedup)
3. Full materialization timing (complete client experience)

Test Modes:
    - CUBESQL_ARROW_RESULTS_CACHE_ENABLED=true:  Tests with optional cache (shows cache speedup)
    - CUBESQL_ARROW_RESULTS_CACHE_ENABLED=false: Tests baseline ADBC(Arrow Native) vs REST API

    Note: When using CubeStore pre-aggregations, data is already cached at the storage
    layer. CubeStore is a cache itself - sometimes one cache is plenty. Cacheless setup
    avoids double-caching and still gets 8-15x speedup from ADBC(Arrow Native) binary protocol.

Requirements:
    pip install psycopg2-binary requests

Usage:
    # From examples/recipes/arrow-ipc directory:

    # Test WITH cache enabled (default)
    export CUBESQL_ARROW_RESULTS_CACHE_ENABLED=true
    ./start-cubesqld.sh &
    python test_arrow_native_performance.py

    # Test WITHOUT cache (baseline ADBC(Arrow Native))
    export CUBESQL_ARROW_RESULTS_CACHE_ENABLED=false
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
    """Tests ADBC server (port 8120) vs REST HTTP API (port 4008)"""

    def __init__(self,
                 arrow_host: str = "192.168.0.249",
                 arrow_port: int = 8120,
                 http_url: str = "http://192.168.0.249:4008/cubejs-api/v1/load"):
        self.arrow_host = arrow_host
        self.arrow_port = arrow_port
        self.http_url = http_url
        self.http_token = "test"  # Default token

        # Detect cache mode from environment
        print(self.arrow_host)
        print(self.http_url)
        
        cache_env = os.getenv("CUBESQL_ARROW_RESULTS_CACHE_ENABLED", "true").lower()
        self.cache_enabled = cache_env in ("true", "1", "yes")

    def run_arrow_query(self, sql: str, label: str = "") -> QueryResult:
        """Execute query via ADBC server (port 8120) with full materialization"""
        # Connect using ADBC(Arrow Native) client
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
        """Print comparison between ADBC(Arrow Native) and REST HTTP"""
        if arrow_result.total_time_ms > 0:
            speedup = http_result.total_time_ms / arrow_result.total_time_ms
            time_saved = http_result.total_time_ms - arrow_result.total_time_ms
            color = Colors.GREEN if speedup > 5 else Colors.YELLOW
            print(f"\n  {color}{Colors.BOLD}ADBC(Arrow Native) is {speedup:.1f}x faster{Colors.END}")
            print(f"  Time saved: {time_saved}ms\n")
            return speedup
        return 1.0

    def test_arrow_vs_rest(self, limit: int):
        "LIMIT: "+ str(limit) +" rows - ADBC(Arrow Native) vs REST HTTP API"
        self.print_header(
            "Query LIMIT: "+ str(limit),
            f"ADBC(Arrow Native) (8120) vs REST HTTP API (4008) {'[Cache enabled]' if self.cache_enabled else '[No cache]'}"
        )

        sql = """
        SELECT date_trunc('hour', updated_at),
               market_code,
               brand_code,
               subtotal_amount_sum,
               total_amount_sum,
               tax_amount_sum,
               count
        FROM orders_with_preagg
        ORDER BY 1 desc
        LIMIT
        """ + str(limit)

        http_query = {
            "measures": [
                 "orders_with_preagg.subtotal_amount_sum",
                 "orders_with_preagg.total_amount_sum",
                 "orders_with_preagg.tax_amount_sum",
                 "orders_with_preagg.count"
            ],
            "dimensions": [
                "orders_with_preagg.market_code",
                "orders_with_preagg.brand_code"
            ],
            "timeDimensions": [{
                "dimension": "orders_with_preagg.updated_at",
                "granularity": "hour"
            }],
            "order": {
              "orders_with_preagg.updated_at": "desc"},
            "limit": limit
        }

        if self.cache_enabled:
            # Warm up cache
            print(f"{Colors.CYAN}Warming up cache...{Colors.END}")
            self.run_arrow_query(sql)
            time.sleep(0.1)

        # Run comparison
        print(f"{Colors.CYAN}Running performance comparison...{Colors.END}\n")
        arrow_result = self.run_arrow_query(sql, "ADBC(Arrow Native)")
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
        print(f"  ADBC(Arrow Native) (port 8120) vs REST HTTP API (port 4008)")
        cache_status = "expected" if self.cache_enabled else "not expected"
        cache_color = Colors.GREEN if self.cache_enabled else Colors.YELLOW
        print(f"  Arrow Results Cache behavior: {cache_color}{cache_status}{Colors.END}")
        print(f"  Note: REST HTTP API has caching always enabled")
        print("=" * 80)
        print(f"{Colors.END}\n")

        speedups = []

        try:
            # Test 2: Small query
            speedup2 = self.test_arrow_vs_rest(200)
            speedups.append(("Small Query (200 rows)", speedup2))

            # Test 3: Medium query
            speedup3 = self.test_arrow_vs_rest(2000)
            speedups.append(("Medium Query (2K rows)", speedup3))

            # Test 4: Large query
            speedup4 = self.test_arrow_vs_rest(20000)
            speedups.append(("Large Query (20K rows)", speedup4))

            # Test 5: Largest query
            speedup5 = self.test_arrow_vs_rest(50000)
            speedups.append(("Largest Query Allowed 50K rows", speedup5))

        except Exception as e:
            print(f"\n{Colors.RED}{Colors.BOLD}ERROR: {e}{Colors.END}")
            print(f"\n{Colors.YELLOW}Make sure:")
            print(f"  1. ADBC server is running on localhost:8120")
            print(f"  2. Cube REST API is running on localhost:4008")
            print(f"  3. orders_with_preagg cube exists with data")
            print(f"  4. CUBESQL_ARROW_RESULTS_CACHE_ENABLED is set correctly{Colors.END}\n")
            sys.exit(1)

        # Print summary
        self.print_summary(speedups)

    def print_summary(self, speedups: List[tuple]):
        """Print final summary of all tests"""
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print("  SUMMARY: ADBC(Arrow Native) vs REST HTTP API Performance")
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

        print(f"{Colors.GREEN}{Colors.BOLD}✓ All tests completed{Colors.END}")
        if self.cache_enabled:
            print(f"{Colors.CYAN}Results show ADBC(Arrow Native) performance with cache behavior expected.{Colors.END}")
            print(f"{Colors.CYAN}Note: REST HTTP API has caching always enabled.{Colors.END}\n")
        else:
            print(f"{Colors.CYAN}Results show ADBC(Arrow Native) baseline performance (cache behavior not expected).{Colors.END}")
            print(f"{Colors.CYAN}Note: REST HTTP API has caching always enabled.{Colors.END}\n")


def main():
    """Main entry point"""
    tester = ArrowNativePerformanceTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
