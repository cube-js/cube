cube('Channels', {
  sql: 'SELECT * FROM channels WHERE is_archived = 0',

  joins: {},

  measures: {
    count: {
      type: 'count',
    }
  },

  dimensions: {
    id: {
      sql: 'id',
      type: 'string',
      primaryKey: true,
      shown: true
    },

    name: {
      sql: 'name',
      type: 'string'
    },

    is_general: {
      sql: 'is_general',
      type: 'boolean'
    },

    purpose: {
      sql: 'purpose',
      type: 'string'
    }
  }
});
