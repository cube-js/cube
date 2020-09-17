cube('Reactions', {
  sql: 'SELECT * FROM reactions',

  joins: {
    Messages: {
      relationship: 'hasOne',
      sql: `${Reactions}.message_id = ${Messages}.id`
    }
  },

  measures: {
    count: {
      type: 'count'
    }
  },

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primaryKey: true
    },
    emoji: {
      sql: 'emoji',
      type: 'string'
    },
    skin_tone: {
      sql: 'skin_tone',
      type: 'string'
    }
  }
});
