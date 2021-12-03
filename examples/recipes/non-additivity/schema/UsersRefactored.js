cube(`UsersRefactored`, {
  extends: Users,

  measures: {
    // Replaced with an approximate distinct count
    // When used with Postgres, requires a HypedLogLog extension:
    // https://github.com/citusdata/postgresql-hll
    // distinctAges: {
    //   sql: `age`,
    //   type: `countDistinctApprox`,
    // },

    // Decomposed into a formula with additive measures
    avgAge: {
      sql: `${CUBE.ageSum} / ${CUBE.count}`,
      type: `number`,
    },

    ageSum: {
      sql: `age`,
      type: `sum`,
    },

    count: {
      type: `count`,
    },
  },

  preAggregations: {
    main: {
      measures: [
        CUBE.ageSum,
        CUBE.count
      ],
      dimensions: [
        CUBE.gender
      ]
    },
  },
});
