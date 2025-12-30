192.168.0.249
http://192.168.0.249:4008/cubejs-api/v1/load

[1m[95m
================================================================================
  CUBESQL ARROW NATIVE SERVER PERFORMANCE TEST SUITE
  ADBC(Arrow Native) (port 8120) vs REST HTTP API (port 4008)
  Arrow Results Cache behavior: [92mexpected[0m
  Note: REST HTTP API has caching always enabled
================================================================================
[0m


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 200[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:   11ms | Materialize:   3ms | Total:   14ms |    200 rows
  REST   | Query:   94ms | Materialize:   3ms | Total:   97ms |    200 rows

  [92m[1mADBC(Arrow Native) is 6.9x faster[0m
  Time saved: 83ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 2000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:   17ms | Materialize:   4ms | Total:   21ms |   2000 rows
  REST   | Query:  157ms | Materialize:  22ms | Total:  179ms |   2000 rows

  [92m[1mADBC(Arrow Native) is 8.5x faster[0m
  Time saved: 158ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 20000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:   72ms | Materialize:   7ms | Total:   79ms |  20000 rows
  REST   | Query:  909ms | Materialize: 116ms | Total: 1025ms |  20000 rows

  [92m[1mADBC(Arrow Native) is 13.0x faster[0m
  Time saved: 946ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 50000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:  101ms | Materialize:  14ms | Total:  115ms |  50000 rows
  REST   | Query: 1609ms | Materialize: 239ms | Total: 1848ms |  50000 rows

  [92m[1mADBC(Arrow Native) is 16.1x faster[0m
  Time saved: 1733ms


[1m[95m
================================================================================
  SUMMARY: ADBC(Arrow Native) vs REST HTTP API Performance
================================================================================
[0m

  Small Query (200 rows)         [92m   6.9x faster[0m
  Medium Query (2K rows)         [92m   8.5x faster[0m
  Large Query (20K rows)         [92m  13.0x faster[0m
  Largest Query Allowed 50K rows [92m  16.1x faster[0m

  [1mAverage Speedup:[0m [92m[1m11.1x[0m

[1m================================================================================[0m

[92m[1m✓ All tests completed[0m
[96mResults show ADBC(Arrow Native) performance with cache behavior expected.[0m
[96mNote: REST HTTP API has caching always enabled.[0m

