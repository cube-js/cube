cube(`HN`, {
  sql: `SELECT * FROM public.hn_top`,

  refreshKey: {
    every: '1 second'
  },
  
  measures: {
    count: {
      type: `count`
    },

    countTop3: {
      type: `count`,
      filters: [ {
        sql: `${rank} <= 3`
      } ]
    },

    bestRank: {
      sql: `rank`,
      type: `min`
    }
  },
  
  dimensions: {
    link: {
      sql: `link`,
      type: `string`
    },

    comments: {
      sql: `comments`,
      type: `string`
    },

    title: {
      sql: `title`,
      type: `string`
    },

    rank: {
      sql: `rank`,
      type: `number`
    }
  },

  segments: {
    show: {
      sql: `${title} LIKE 'Show HN:%'`
    }
  }
});
