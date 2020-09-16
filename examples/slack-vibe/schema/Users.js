cube('Users', {
  sql: 'SELECT * FROM users WHERE deleted = 0',

  joins: {
    Reactions: {
      relationship: 'hasMany',
      sql: `${Users}.id = ${Reactions}.user_id`,
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  dimensions: {
    id: {
      sql: 'id',
      type: 'string',
      primaryKey: true,
      shown: true,
    },
    name: {
      sql: 'name',
      type: 'string',
    },
    title: {
      sql: 'title',
      type: 'string',
    },
    real_name: {
      sql: 'real_name',
      type: 'string',
    },
    image: {
      sql: 'image_512',
      type: 'string',
    },
    is_admin: {
      sql: 'is_admin',
      type: 'boolean',
    },
    tz: {
      sql: 'tz',
      type: 'string',
    },
    tz_offset: {
      sql: 'tz_offset',
      type: 'string',
    },
  },

  segments: {
    admin: {
      sql: `${Users}.is_admin = 1`,
    },
    regular: {
      sql: `${Users}.is_admin = 0`,
    },
  },
});
