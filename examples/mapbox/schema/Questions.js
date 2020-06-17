cube(`Questions`, {
  sql: `SELECT * FROM public.Questions`,
  dataSource: `mapbox__example`,
  joins: {
    Users: {
      sql: `${CUBE}.owner_user_id = ${Users}.id`,
      relationship: `belongsTo`,
    },
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
    title: {
      sql: `title`,
      type: `string`
    },
    tags: {
      sql: `tags`,
      type: `string`
    },
    views: {
      sql: `view_count`,
      type: `number`,
    }
  },
  preAggregations: {
    questions: {
      type: `rollup`,
      dimensionReferences: [Questions.id, Questions.tags, Questions.title, Questions.views, Users.geometry]
    }
  }
});