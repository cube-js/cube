cube(`ActiveUsers`, {
  sql: `SELECT user_id, created_at FROM public.orders`,

  measures: {
    monthlyActiveUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `30 day`,
        offset: `start`,
      },
    },

    weeklyActiveUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `7 day`,
        offset: `start`,
      },
    },

    dailyActiveUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `1 day`,
        offset: `start`,
      },
    },

    wauToMau: {
      title: `WAU to MAU`,
      sql: `100.000 * ${weeklyActiveUsers} / NULLIF(${monthlyActiveUsers}, 0)`,
      type: `number`,
      format: `percent`,
    },
  },

  dimensions: {
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
