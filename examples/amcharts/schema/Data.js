cube(`Data`, {
  sql: `SELECT * FROM public.data`,

  joins: {
    /*Users: {
      sql: `${CUBE}.user = ${Users}.id`,
      relationship: `belongsTo`,
    },*/
  },

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    clientMsgId: {
      sql: `client_msg_id`,
      type: `string`,
      //primaryKey: true,
    },

    subtype: {
      sql: `subtype`,
      type: `string`,
    },

    text: {
      sql: `text`,
      type: `string`,
    },

    user: {
      sql: `user`,
      type: `string`,
    },

    type: {
      sql: `type`,
      type: `string`,
    },

    ts: {
      sql: `TO_TIMESTAMP(ts)`,
      type: `time`,
    },
  },
});
