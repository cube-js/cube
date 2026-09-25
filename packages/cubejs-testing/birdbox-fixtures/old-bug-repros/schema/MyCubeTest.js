// cube-js/cube#8916: rollup containing only `count_distinct_approx` measures.
// `my_cube_test_ctl` is the control from the issue (adding a `count` makes it build).
// Requires the postgresql-hll extension (or the shim installed by the test).
cube(`my_cube_test`, {
  sql: `
    SELECT 123 AS abc UNION ALL
    SELECT 234 AS abc UNION ALL
    SELECT 345 AS abc
  `,
  measures: {
    mes: { sql: `abc`, type: `count_distinct_approx` }
  },
  pre_aggregations: {
    main: { measures: [ mes ] }
  }
})

cube(`my_cube_test_ctl`, {
  sql: `SELECT 123 AS abc UNION ALL SELECT 234 AS abc UNION ALL SELECT 345 AS abc`,
  measures: {
    mes: { sql: `abc`, type: `count_distinct_approx` },
    cnt: { type: `count` },
    exact: { sql: `abc`, type: `count_distinct` },
  },
  pre_aggregations: {
    main: { measures: [ mes, cnt ] }
  }
})
