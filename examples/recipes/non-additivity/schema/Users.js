cube(`Users`, {
  sql: `SELECT * FROM public.users`,
  
  dimensions: {
    name: {
      sql: `last_name || ', ' || first_name`,
      type: `string`
    },

    age: {
      sql: `age`,
      type: `number`,
    },

    gender: {
      sql: `gender`,
      type: `string`,
    },
  },

  measures: {
    distinctAges: {
      sql: `age`,
      type: `countDistinct`,
    },

    avgAge: {
      sql: `age`,
      type: `avg`,
    },

    p90Age: {
      sql: `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY age)`,
      type: `number`,
    },
  },

  preAggregations: {
    main: {
      measures: [
        CUBE.distinctAges,
        CUBE.avgAge,
        CUBE.p90Age
      ],
      dimensions: [
        CUBE.gender
      ]
    },
  },
});
