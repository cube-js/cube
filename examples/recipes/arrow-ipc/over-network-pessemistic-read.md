# REMOTE

"""
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

  ARROW  | Query:   84ms | Materialize:   4ms | Total:   88ms |    200 rows
  REST   | Query:   83ms | Materialize:   3ms | Total:   86ms |    200 rows

  [93m[1mADBC(Arrow Native) is 1.0x faster[0m
  Time saved: -2ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 2000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:  232ms | Materialize:   3ms | Total:  235ms |   2000 rows
  REST   | Query:  194ms | Materialize:  26ms | Total:  220ms |   2000 rows

  [93m[1mADBC(Arrow Native) is 0.9x faster[0m
  Time saved: -15ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 20000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:  865ms | Materialize:  11ms | Total:  876ms |  20000 rows
  REST   | Query:  751ms | Materialize: 112ms | Total:  863ms |  20000 rows

  [93m[1mADBC(Arrow Native) is 1.0x faster[0m
  Time saved: -13ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 50000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query: 2035ms | Materialize:  21ms | Total: 2056ms |  50000 rows
  REST   | Query: 1483ms | Materialize: 246ms | Total: 1729ms |  50000 rows

  [93m[1mADBC(Arrow Native) is 0.8x faster[0m
  Time saved: -327ms


[1m[95m
================================================================================
  SUMMARY: ADBC(Arrow Native) vs REST HTTP API Performance
================================================================================
[0m

  Small Query (200 rows)         [93m   1.0x faster[0m
  Medium Query (2K rows)         [93m   0.9x faster[0m
  Large Query (20K rows)         [93m   1.0x faster[0m
  Largest Query Allowed 50K rows [93m   0.8x faster[0m

  [1mAverage Speedup:[0m [92m[1m0.9x[0m

[1m================================================================================[0m

[92m[1m✓ All tests completed[0m
[96mResults show ADBC(Arrow Native) performance with cache behavior expected.[0m
[96mNote: REST HTTP API has caching always enabled.[0m

"""
