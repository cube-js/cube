cube(`Answers`, {
  sql: `SELECT * FROM public.Answers`,
  joins: {
    Users: {
      sql: `${CUBE}.owner_user_id = ${Users}.id`,
      relationship: `belongsTo`,
    },
  },
  measures: {
    count: {
      type: `count`,
    }
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    }
  },
  preAggregations: {
    answersCount: {
      type: `rollup`,
      measureReferences: [Answers.count],
      dimensionReferences: [Users.geometry]
    }
  }
});