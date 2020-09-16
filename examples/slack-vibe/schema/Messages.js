cube('Messages', {
  sql: 'SELECT * FROM messages WHERE subtype is null',

  joins: {
    Channels: {
      relationship: 'hasOne',
      sql: `${Messages}.channel_id = ${Channels}.id`
    },
    Users: {
      relationship: 'hasOne',
      sql: `${Messages}.user_id = ${Users}.id`
    },
    Reactions: {
      relationship: 'hasMany',
      sql: `${Messages}.id = ${Reactions}.message_id`
    }
  },

  measures: {
    count: {
      type: 'count'
    },

    avg_text_size: {
      type: 'avg',
      sql: 'LENGTH(text)'
    }
  },

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primaryKey: true
    },

    channel_id: {
      sql: 'channel_id',
      type: 'string'
    },

    text: {
      sql: 'text',
      type: 'boolean'
    },

    user_id: {
      sql: 'user_id',
      type: 'string'
    },

    date: {
      sql: `DATE(${Messages}.ts, 'unixepoch')`,
      type: 'time'
    },

    date_time: {
      sql: `DATETIME(${Messages}.ts, 'unixepoch')`,
      type: 'time'
    },

    hour: {
      sql: `STRFTIME('%H', ${Messages}.ts, 'unixepoch')`,
      type: 'number'
    },

    day_of_week: {
      sql: `STRFTIME('%w', ${Messages}.ts, 'unixepoch')`,
      type: 'number'
    },

    week_year: {
      sql: `STRFTIME('%Y-%W', ${Messages}.ts, 'unixepoch')`,
      type: 'number'
    },
  }
});
