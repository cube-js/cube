cube(`Answers`, {
  sql: `SELECT * FROM public.Answers`,
  dataSource: `mapbox__example`,
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
    },
    owner_user_id: {
      sql: `owner_user_id`,
      type: `number`,
    },
  },
  preAggregations: {
    answersCount: {
      type: `rollup`,
      measureReferences: [Answers.count],
      dimensionReferences: [Users.geometry]
    }
  }
});