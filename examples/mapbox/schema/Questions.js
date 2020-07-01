cube(`Questions`, {
  sql: `SELECT * FROM public.Questions`,
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