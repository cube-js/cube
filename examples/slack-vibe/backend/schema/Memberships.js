cube('Memberships', {
  sql: 'SELECT * FROM messages WHERE subtype IN(\'channel_join\', \'channel_leave\')',

  joins: {
    Channels: {
      relationship: 'hasOne',
      sql: `${Memberships}.channel_id = ${Channels}.id`
    },
    Users: {
      relationship: 'hasOne',
      sql: `${Memberships}.user_id = ${Users}.id`
    }
  },

  measures: {
    count: {
      sql: 'CASE subtype = \'channel_join\' WHEN 1 THEN 1 ELSE -1 END',
      type: 'count'
    },

    sum: {
      sql: 'CASE subtype = \'channel_join\' WHEN 1 THEN 1 ELSE -1 END',
      type: 'sum',
      rollingWindow: {
        trailing: 'unbounded',
      },
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

    user_id: {
      sql: 'user_id',
      type: 'string'
    },

    date: {
      sql: `DATE(${Memberships}.ts, 'unixepoch')`,
      type: 'time'
    },

    date_time: {
      sql: `DATETIME(${Memberships}.ts, 'unixepoch')`,
      type: 'time'
    }
  }
});
