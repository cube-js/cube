cube(`Tweets`, {
  sql: `select * from tweets`,

  measures: {
    count: {
      type: `count`
    },

    favoriteCount: {
      type: `sum`,
      sql: `favorite_count`
    },

    retweetCount: {
      type: `sum`,
      sql: `retweet_count`
    }
  },

  dimensions: {
    location: {
      type: `string`,
      sql: `\`user.location\``
    },

    lang: {
      type: `string`,
      sql: `lang`
    }
  }
});
