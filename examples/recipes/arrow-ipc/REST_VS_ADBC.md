localhost
http://localhost:4008/cubejs-api/v1/load

[1m[95m
================================================================================
  CUBESQL ARROW NATIVE SERVER PERFORMANCE TEST SUITE
  ADBC(Arrow Native) (port 8120) vs REST HTTP API (port 4008)
  Arrow Results Cache behavior: [92mexpected[0m
  Note: REST HTTP API has caching always enabled
================================================================================
[0m


[1m[94m================================================================================[0m
[1m[94mTEST: Variety Suite (mandata_captate)[0m
[96m32 query variants | ADBC(Arrow Native) vs REST HTTP[0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

  ARROW  | Query:    2ms | Materialize:   0ms | Total:    2ms |    222 rows
  REST   | Query:   80ms | Materialize:   0ms | Total:   80ms |    222 rows
  ARROW  | Query:   40ms | Materialize:   1ms | Total:   41ms |     52 rows
  REST   | Query:   66ms | Materialize:   0ms | Total:   66ms |     52 rows
  ARROW  | Query:    0ms | Materialize:   1ms | Total:    1ms |   2188 rows
  REST   | Query:  268ms | Materialize:   7ms | Total:  275ms |   2188 rows
  ARROW  | Query:   40ms | Materialize:   1ms | Total:   41ms |     37 rows
  REST   | Query:  249ms | Materialize:   0ms | Total:  249ms |     37 rows
  ARROW  | Query:   41ms | Materialize:   1ms | Total:   42ms |     91 rows
  REST   | Query:   56ms | Materialize:   0ms | Total:   56ms |     91 rows
  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   4394 rows
  REST   | Query: 1250ms | Materialize:  14ms | Total: 1264ms |   4394 rows
  ARROW  | Query:   41ms | Materialize:   1ms | Total:   42ms |      2 rows
  REST   | Query:   63ms | Materialize:   0ms | Total:   63ms |      2 rows
  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   3153 rows
  REST   | Query:  302ms | Materialize:  10ms | Total:  312ms |   3153 rows
  ARROW  | Query:   40ms | Materialize:   0ms | Total:   40ms |      1 rows
  REST   | Query:   62ms | Materialize:   0ms | Total:   62ms |      1 rows
  ARROW  | Query:   41ms | Materialize:   0ms | Total:   41ms |    356 rows
  REST   | Query:   57ms | Materialize:   0ms | Total:   57ms |    356 rows
  ARROW  | Query:   41ms | Materialize:   1ms | Total:   42ms |     52 rows
  REST   | Query:  918ms | Materialize:   0ms | Total:  918ms |     52 rows
  ARROW  | Query:    2ms | Materialize:   2ms | Total:    4ms |   8143 rows
  REST   | Query:  595ms | Materialize:  39ms | Total:  634ms |   8143 rows
  ARROW  | Query:    3ms | Materialize:   2ms | Total:    5ms |   6284 rows
  REST   | Query:  581ms | Materialize:  33ms | Total:  614ms |   6284 rows
  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   2045 rows
  REST   | Query: 1047ms | Materialize:   6ms | Total: 1053ms |   2045 rows
  ARROW  | Query:    6ms | Materialize:   4ms | Total:   10ms |  28000 rows
  REST   | Query:  835ms | Materialize:  65ms | Total:  900ms |  28000 rows
  ARROW  | Query:    2ms | Materialize:   1ms | Total:    3ms |   4000 rows
  REST   | Query:  375ms | Materialize:  12ms | Total:  387ms |   4000 rows
  ARROW  | Query:    3ms | Materialize:   2ms | Total:    5ms |  26714 rows
  REST   | Query:  749ms | Materialize:  63ms | Total:  812ms |  26714 rows
  ARROW  | Query:   41ms | Materialize:   0ms | Total:   41ms |     91 rows
  REST   | Query:   62ms | Materialize:   0ms | Total:   62ms |     91 rows
  ARROW  | Query:    4ms | Materialize:   2ms | Total:    6ms |  10000 rows
  REST   | Query:  514ms | Materialize:  29ms | Total:  543ms |  10000 rows
  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   2188 rows
  REST   | Query:  325ms | Materialize:   8ms | Total:  333ms |   2188 rows
  ARROW  | Query:   41ms | Materialize:   0ms | Total:   41ms |      1 rows
  REST   | Query:  827ms | Materialize:   0ms | Total:  827ms |      1 rows
  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   1755 rows
  REST   | Query:  365ms | Materialize:   5ms | Total:  370ms |   1755 rows
  ARROW  | Query:   40ms | Materialize:   0ms | Total:   40ms |      4 rows
  REST   | Query:   61ms | Materialize:   0ms | Total:   61ms |      4 rows
  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   1820 rows
  REST   | Query:  447ms | Materialize:   7ms | Total:  454ms |   1820 rows
  ARROW  | Query:   41ms | Materialize:   1ms | Total:   42ms |     14 rows
  REST   | Query:   62ms | Materialize:   0ms | Total:   62ms |     14 rows
  ARROW  | Query:   41ms | Materialize:   0ms | Total:   41ms |      4 rows
  REST   | Query:   52ms | Materialize:   0ms | Total:   52ms |      4 rows
  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   3153 rows
  REST   | Query:  980ms | Materialize:   8ms | Total:  988ms |   3153 rows
  ARROW  | Query:   41ms | Materialize:   1ms | Total:   42ms |      9 rows
  REST   | Query:  356ms | Materialize:   0ms | Total:  356ms |      9 rows
  ARROW  | Query:    1ms | Materialize:   2ms | Total:    3ms |   8454 rows
  REST   | Query:  468ms | Materialize:  24ms | Total:  492ms |   8454 rows
  ARROW  | Query:    6ms | Materialize:   4ms | Total:   10ms |  18000 rows
  REST   | Query:  961ms | Materialize:  64ms | Total: 1025ms |  18000 rows
  ARROW  | Query:   41ms | Materialize:   0ms | Total:   41ms |     12 rows
  REST   | Query:  221ms | Materialize:   0ms | Total:  221ms |     12 rows
  ARROW  | Query:   41ms | Materialize:   1ms | Total:   42ms |     14 rows
  REST   | Query:  892ms | Materialize:   0ms | Total:  892ms |     14 rows

  [1mVariety summary:[0m
  Avg speedup: 119.31x | P50: 65.00x | P95: 508.62x
  Avg ADBC total: 21ms
  Avg REST total: 454ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 200[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:   41ms | Materialize:   1ms | Total:   42ms |    200 rows
  REST   | Query: 1413ms | Materialize:   1ms | Total: 1414ms |    200 rows

  [92m[1mADBC(Arrow Native) is 33.7x faster[0m
  Time saved: 1372ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 2000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:    1ms | Materialize:   1ms | Total:    2ms |   2000 rows
  REST   | Query: 1568ms | Materialize:   8ms | Total: 1576ms |   2000 rows

  [92m[1mADBC(Arrow Native) is 788.0x faster[0m
  Time saved: 1574ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 20000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:    5ms | Materialize:   3ms | Total:    8ms |  20000 rows
  REST   | Query: 2067ms | Materialize:  66ms | Total: 2133ms |  20000 rows

  [92m[1mADBC(Arrow Native) is 266.6x faster[0m
  Time saved: 2125ms


[1m[94m================================================================================[0m
[1m[94mTEST: Query LIMIT: 50000[0m
[96mADBC(Arrow Native) (8120) vs REST HTTP API (4008) [Cache enabled][0m
[1m[94m────────────────────────────────────────────────────────────────────────────────[0m

[96mWarming up cache...[0m
[96mRunning performance comparison...[0m

  ARROW  | Query:   15ms | Materialize:   6ms | Total:   21ms |  50000 rows
  REST   | Query: 2420ms | Materialize: 162ms | Total: 2582ms |  50000 rows

  [92m[1mADBC(Arrow Native) is 123.0x faster[0m
  Time saved: 2561ms


[1m[95m
================================================================================
  SUMMARY: ADBC(Arrow Native) vs REST HTTP API Performance
================================================================================
[0m

  Small Query (200 rows)         [92m  33.7x faster[0m
  Medium Query (2K rows)         [92m 788.0x faster[0m
  Large Query (20K rows)         [92m 266.6x faster[0m
  Largest Query Allowed 50K rows [92m 123.0x faster[0m

  [1mAverage Speedup:[0m [92m[1m302.8x[0m

[1m================================================================================[0m

[92m[1m✓ All tests completed[0m
[96mResults show ADBC(Arrow Native) performance with cache behavior expected.[0m
[96mNote: REST HTTP API has caching always enabled.[0m

