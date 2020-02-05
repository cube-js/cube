cube(`Users`, {
  sql: `
    SELECT
     e.domain_userid
   FROM ${Events.sql()} AS e
   WHERE e.session_index = 1
  `,

  measures: {
    count: {
      type: `count`
    },

    averageNumberSessions: {
      type: `avg`,
      sql: `${numberSessions}`
    }
  },

  dimensions: {
    numberSessions: {
      sql: `${Sessions.count}`,
      type: `number`,
      subQuery: true
    },

    id: {
      sql: `domain_userid`,
      type: `string`,
      primaryKey: true
    }
  }
});
