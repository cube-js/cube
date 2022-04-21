cube(`Events`, {
  sql: `SELECT * FROM events`,

  joins: {

  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    }
  },

  dimensions: {
    actor: {
      sql: `actor`,
      type: `string`
    },

    public: {
      sql: `public`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    type: {
      sql: `type`,
      type: `string`
    },

    payload: {
      sql: `payload`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },

  dataSource: `default`
});
