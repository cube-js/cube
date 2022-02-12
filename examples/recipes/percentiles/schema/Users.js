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
  },

  measures: {
    avgAge: {
      sql: `age`,
      type: `avg`,
    },

    medianAge: {
      sql: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age)`,
      type: `number`,
    },

    p95Age: {
      sql: `PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY age)`,
      type: `number`,
    },
  },
});