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
import random
from dataclasses import dataclass
from typing import List, Dict, Any, Iterable, Tuple
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


@dataclass
class QueryVariant:
    """Pair of SQL + HTTP queries for comparison"""
    label: str
    sql: str
    http_query: Dict[str, Any]


class ArrowNativePerformanceTester:
    """Tests ADBC server (port 8120) vs REST HTTP API (port 4008)"""

    def __init__(self,
                 arrow_host: str = "localhost", #"192.168.0.249",
                 arrow_port: int = 8120,
                 http_url: str = "http://localhost:4008/cubejs-api/v1/load"  # "http://192.168.0.249:4008/cubejs-api/v1/load"
                 ):
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

        base_set = os.getenv("ARROW_TEST_BASE_SET", "mandata_captate").strip().lower()
        sql, http_query = build_base_queries(base_set, limit)

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

    def test_variety_suite(self, variants: List[QueryVariant], label: str):
        """Run a variety of queries and summarize aggregate speedups."""
        self.print_header(
            label,
            f"{len(variants)} query variants | ADBC(Arrow Native) vs REST HTTP"
        )

        speedups = []
        arrow_totals = []
        rest_totals = []

        for variant in variants:
            if self.cache_enabled:
                self.run_arrow_query(variant.sql)
                time.sleep(0.05)

            arrow_result = self.run_arrow_query(variant.sql, f"ADBC: {variant.label}")
            rest_result = self.run_http_query(variant.http_query, f"REST: {variant.label}")

            self.print_result(arrow_result, "  ")
            self.print_result(rest_result, "  ")

            if arrow_result.total_time_ms > 0:
                speedups.append(rest_result.total_time_ms / arrow_result.total_time_ms)
                arrow_totals.append(arrow_result.total_time_ms)
                rest_totals.append(rest_result.total_time_ms)

        if speedups:
            avg_speedup = sum(speedups) / len(speedups)
            p50 = percentile(speedups, 50)
            p95 = percentile(speedups, 95)
            print(f"\n  {Colors.BOLD}Variety summary:{Colors.END}")
            print(f"  Avg speedup: {avg_speedup:.2f}x | P50: {p50:.2f}x | P95: {p95:.2f}x")
            print(f"  Avg ADBC total: {int(sum(arrow_totals) / len(arrow_totals))}ms")
            print(f"  Avg REST total: {int(sum(rest_totals) / len(rest_totals))}ms\n")

        return speedups

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
            variant_set = os.getenv("ARROW_TEST_QUERY_SET", "mandata_captate").strip().lower()
            variant_count = int(os.getenv("ARROW_TEST_VARIANT_COUNT", "32"))
            variant_seed = int(os.getenv("ARROW_TEST_VARIANT_SEED", "42"))

            variants = pick_variants(
                get_variants(variant_set),
                variant_count,
                variant_seed
            )
            self.test_variety_suite(variants, f"Variety Suite ({variant_set})")

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


def percentile(values: List[float], pct: int) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    k = (len(values_sorted) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(values_sorted) - 1)
    if f == c:
        return values_sorted[f]
    d0 = values_sorted[f] * (c - k)
    d1 = values_sorted[c] * (k - f)
    return d0 + d1


def pick_variants(variants: List[QueryVariant], count: int, seed: int) -> List[QueryVariant]:
    if count <= 0:
        return []
    if count >= len(variants):
        return variants
    rng = random.Random(seed)
    return rng.sample(variants, count)


def get_variants(name: str) -> List[QueryVariant]:
    if name == "mandata_captate":
        return generate_mandata_captate_variants()
    if name == "orders_with_preagg":
        return generate_orders_with_preagg_variants()
    raise ValueError(f"Unknown query set: {name}")


def generate_orders_with_preagg_variants() -> List[QueryVariant]:
    variants = []
    limits = [50, 100, 200, 500, 1000]
    granularities = ["day", "hour"]
    date_ranges = [
        ("2024-01-01", "2024-12-31"),
        ("2023-01-01", "2023-12-31"),
    ]

    template_sql = [
        ("brand", "SELECT orders_with_preagg.brand_code, MEASURE(orders_with_preagg.count) FROM orders_with_preagg GROUP BY 1 LIMIT {limit}",
         {"dimensions": ["orders_with_preagg.brand_code"], "measures": ["orders_with_preagg.count"]}),
        ("market", "SELECT orders_with_preagg.market_code, MEASURE(orders_with_preagg.count), MEASURE(orders_with_preagg.total_amount_sum) FROM orders_with_preagg GROUP BY 1 LIMIT {limit}",
         {"dimensions": ["orders_with_preagg.market_code"], "measures": ["orders_with_preagg.count", "orders_with_preagg.total_amount_sum"]}),
        ("market_brand", "SELECT orders_with_preagg.market_code, orders_with_preagg.brand_code, MEASURE(orders_with_preagg.count), MEASURE(orders_with_preagg.tax_amount_sum) FROM orders_with_preagg GROUP BY 1, 2 LIMIT {limit}",
         {"dimensions": ["orders_with_preagg.market_code", "orders_with_preagg.brand_code"], "measures": ["orders_with_preagg.count", "orders_with_preagg.tax_amount_sum"]}),
    ]

    for granularity in granularities:
        for start, end in date_ranges:
            for limit in limits:
                time_dim = {
                    "dimension": "orders_with_preagg.updated_at",
                    "granularity": granularity,
                    "dateRange": [start, end],
                }
                for label, sql_tmpl, http_base in template_sql:
                    sql = (
                        f"SELECT DATE_TRUNC('{granularity}', orders_with_preagg.updated_at), "
                        f"{sql_tmpl.format(limit=limit).split('SELECT ')[1]}"
                    )
                    http_query = dict(http_base)
                    http_query["timeDimensions"] = [time_dim]
                    http_query["limit"] = limit
                    variants.append(QueryVariant(
                        label=f"{label}:{granularity}:{start}->{end}:L{limit}",
                        sql=sql,
                        http_query=http_query,
                    ))

    return variants


def build_base_queries(base_set: str, limit: int) -> Tuple[str, Dict[str, Any]]:
    if base_set == "orders_with_preagg":
        sql = (
            "SELECT DATE_TRUNC('hour', orders_with_preagg.updated_at), "
            "orders_with_preagg.market_code, "
            "orders_with_preagg.brand_code, "
            "MEASURE(orders_with_preagg.subtotal_amount_sum), "
            "MEASURE(orders_with_preagg.total_amount_sum), "
            "MEASURE(orders_with_preagg.tax_amount_sum), "
            "MEASURE(orders_with_preagg.count) "
            "FROM orders_with_preagg "
            "GROUP BY 1, 2, 3 "
            f"LIMIT {limit}"
        )
        http_query = {
            "measures": [
                "orders_with_preagg.subtotal_amount_sum",
                "orders_with_preagg.total_amount_sum",
                "orders_with_preagg.tax_amount_sum",
                "orders_with_preagg.count",
            ],
            "dimensions": [
                "orders_with_preagg.market_code",
                "orders_with_preagg.brand_code",
            ],
            "timeDimensions": [{
                "dimension": "orders_with_preagg.updated_at",
                "granularity": "hour",
            }],
            "limit": limit,
        }
        return sql, http_query

    if base_set == "mandata_captate":
        sql = (
            "SELECT DATE_TRUNC('hour', mandata_captate.updated_at), "
            "mandata_captate.market_code, "
            "mandata_captate.brand_code, "
            "MEASURE(mandata_captate.total_amount_sum), "
            "MEASURE(mandata_captate.tax_amount_sum), "
            "MEASURE(mandata_captate.count) "
            "FROM mandata_captate "
            "WHERE mandata_captate.updated_at >= '2024-01-01' "
            "AND mandata_captate.updated_at <= '2024-12-31' "
            "GROUP BY 1, 2, 3 "
            f"LIMIT {limit}"
        )
        http_query = {
            "measures": [
                "mandata_captate.total_amount_sum",
                "mandata_captate.tax_amount_sum",
                "mandata_captate.count",
            ],
            "dimensions": [
                "mandata_captate.market_code",
                "mandata_captate.brand_code",
            ],
            "timeDimensions": [{
                "dimension": "mandata_captate.updated_at",
                "granularity": "hour",
                "dateRange": ["2024-01-01", "2024-12-31"],
            }],
            "limit": limit,
        }
        return sql, http_query

    raise ValueError(f"Unknown base query set: {base_set}")


def generate_mandata_captate_variants(limit: int = 512) -> List[QueryVariant]:
    limit_values = [i * 1000 for i in range(1, 51)]
    date_ranges = []

    for year in range(2016, 2026):
        date_ranges.append((f"{year}", f"{year}-01-01", f"{year}-12-31"))
        date_ranges.append((f"{year}-H1", f"{year}-01-01", f"{year}-06-30"))
        date_ranges.append((f"{year}-H2", f"{year}-07-01", f"{year}-12-31"))
        for q in range(1, 5):
            sm, em, ed = {
                1: ("01", "03", "31"),
                2: ("04", "06", "30"),
                3: ("07", "09", "30"),
                4: ("10", "12", "31"),
            }[q]
            date_ranges.append((f"{year}-Q{q}", f"{year}-{sm}-01", f"{year}-{em}-{ed}"))

    date_ranges.extend([
        ("Last1Y", "2024-01-01", "2025-12-31"),
        ("Last2Y", "2023-01-01", "2025-12-31"),
        ("Last3Y", "2022-01-01", "2025-12-31"),
        ("Last5Y", "2020-01-01", "2025-12-31"),
        ("AllTime", "2016-01-01", "2025-12-31"),
    ])

    granularities = ["year", "quarter", "month", "week", "day", "hour"]

    def build_sql(template_id: int, granularity: str, start: str, end: str, limit_val: int) -> str:
        base = f"SELECT DATE_TRUNC('{granularity}', mandata_captate.updated_at)"
        where = f"WHERE mandata_captate.updated_at >= '{start}' AND mandata_captate.updated_at <= '{end}'"

        if template_id == 1:
            return f"{base}, MEASURE(mandata_captate.count) FROM mandata_captate {where} GROUP BY 1 LIMIT {limit_val}"
        if template_id == 2:
            return f"{base}, MEASURE(mandata_captate.count), MEASURE(mandata_captate.total_amount_sum), MEASURE(mandata_captate.tax_amount_sum) FROM mandata_captate {where} GROUP BY 1 LIMIT {limit_val}"
        if template_id == 3:
            return f"{base}, mandata_captate.brand_code, MEASURE(mandata_captate.count), MEASURE(mandata_captate.total_amount_sum) FROM mandata_captate {where} GROUP BY 1, 2 LIMIT {limit_val}"
        if template_id == 4:
            return f"{base}, mandata_captate.market_code, mandata_captate.brand_code, MEASURE(mandata_captate.count) FROM mandata_captate {where} GROUP BY 1, 2, 3 LIMIT {limit_val}"
        if template_id == 5:
            return f"{base}, MEASURE(mandata_captate.total_amount_sum), MEASURE(mandata_captate.subtotal_amount_sum), MEASURE(mandata_captate.tax_amount_sum), MEASURE(mandata_captate.discount_total_amount_sum) FROM mandata_captate {where} GROUP BY 1 LIMIT {limit_val}"
        if template_id == 6:
            return f"{base}, mandata_captate.financial_status, MEASURE(mandata_captate.count), MEASURE(mandata_captate.total_amount_sum) FROM mandata_captate {where} GROUP BY 1, 2 LIMIT {limit_val}"
        if template_id == 7:
            return f"{base}, MEASURE(mandata_captate.count), MEASURE(mandata_captate.customer_id_sum), MEASURE(mandata_captate.customer_id_distinct) FROM mandata_captate {where} GROUP BY 1 LIMIT {limit_val}"
        return f"{base}, mandata_captate.market_code, mandata_captate.brand_code, mandata_captate.financial_status, MEASURE(mandata_captate.count), MEASURE(mandata_captate.total_amount_sum) FROM mandata_captate {where} GROUP BY 1, 2, 3, 4 LIMIT {limit_val}"

    def build_http(template_id: int, granularity: str, start: str, end: str, limit_val: int) -> Dict[str, Any]:
        time_dim = {
            "dimension": "mandata_captate.updated_at",
            "granularity": granularity,
            "dateRange": [start, end],
        }

        if template_id == 1:
            return {"measures": ["mandata_captate.count"], "timeDimensions": [time_dim], "limit": limit_val}
        if template_id == 2:
            return {"measures": ["mandata_captate.count", "mandata_captate.total_amount_sum", "mandata_captate.tax_amount_sum"], "timeDimensions": [time_dim], "limit": limit_val}
        if template_id == 3:
            return {"dimensions": ["mandata_captate.brand_code"], "measures": ["mandata_captate.count", "mandata_captate.total_amount_sum"], "timeDimensions": [time_dim], "limit": limit_val}
        if template_id == 4:
            return {"dimensions": ["mandata_captate.market_code", "mandata_captate.brand_code"], "measures": ["mandata_captate.count"], "timeDimensions": [time_dim], "limit": limit_val}
        if template_id == 5:
            return {"measures": ["mandata_captate.total_amount_sum", "mandata_captate.subtotal_amount_sum", "mandata_captate.tax_amount_sum", "mandata_captate.discount_total_amount_sum"], "timeDimensions": [time_dim], "limit": limit_val}
        if template_id == 6:
            return {"dimensions": ["mandata_captate.financial_status"], "measures": ["mandata_captate.count", "mandata_captate.total_amount_sum"], "timeDimensions": [time_dim], "limit": limit_val}
        if template_id == 7:
            return {"measures": ["mandata_captate.count", "mandata_captate.customer_id_sum", "mandata_captate.customer_id_distinct"], "timeDimensions": [time_dim], "limit": limit_val}
        return {"dimensions": ["mandata_captate.market_code", "mandata_captate.brand_code", "mandata_captate.financial_status"], "measures": ["mandata_captate.count", "mandata_captate.total_amount_sum"], "timeDimensions": [time_dim], "limit": limit_val}

    variants = []
    for date_idx, (_label, start, end) in enumerate(date_ranges):
        for gran_idx, granularity in enumerate(granularities):
            for template_id in range(1, 9):
                query_idx = date_idx * 48 + gran_idx * 8 + (template_id - 1)
                limit_val = limit_values[query_idx % len(limit_values)]
                label = f"{granularity}:{start}->{end}:t{template_id}:L{limit_val}"
                variants.append(QueryVariant(
                    label=label,
                    sql=build_sql(template_id, granularity, start, end, limit_val),
                    http_query=build_http(template_id, granularity, start, end, limit_val),
                ))

    return variants[:limit]


def main():
    """Main entry point"""
    tester = ArrowNativePerformanceTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
