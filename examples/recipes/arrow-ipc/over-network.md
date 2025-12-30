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

  ARROW  | Query:   10ms | Materialize:   3ms | Total:   13ms |    200 rows
  REST   | Query:  124ms | Materialize:   3ms | Total:  127ms |    200 rows

  [92m[1mADBC(Arrow Native) is 9.8x faster[0m
  Time saved: 114ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 2000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:   30ms | Materialize:   4ms | Total:   34ms |   2000 rows
  REST   | Query:  275ms | Materialize:  27ms | Total:  302ms |   2000 rows

  [92m[1mADBC(Arrow Native) is 8.9x faster[0m
  Time saved: 268ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 20000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:   76ms | Materialize:   9ms | Total:   85ms |  20000 rows
  REST   | Query:  919ms | Materialize: 129ms | Total: 1048ms |  20000 rows

  [92m[1mADBC(Arrow Native) is 12.3x faster[0m
  Time saved: 963ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 50000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:  104ms | Materialize:  17ms | Total:  121ms |  50000 rows
  REST   | Query: 1652ms | Materialize: 262ms | Total: 1914ms |  50000 rows

  [92m[1mADBC(Arrow Native) is 15.8x faster[0m
  Time saved: 1793ms


[1m[95m
================================================================================
  SUMMARY: ADBC(Arrow Native) vs REST HTTP API Performance
================================================================================
[0m

  Small Query (200 rows)         [92m   9.8x faster[0m
  Medium Query (2K rows)         [92m   8.9x faster[0m
  Large Query (20K rows)         [92m  12.3x faster[0m
  Largest Query Allowed 50K rows [92m  15.8x faster[0m

  [1mAverage Speedup:[0m [92m[1m11.7x[0m

[1m================================================================================[0m

[92m[1m✓ All tests completed[0m
[96mResults show ADBC(Arrow Native) performance with cache behavior expected.[0m
[96mNote: REST HTTP API has caching always enabled.[0m

