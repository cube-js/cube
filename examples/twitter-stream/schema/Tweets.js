cube(`Tweets`, {
  sql: 'SELECT * FROM STATS_4',
  
  measures: {
    count: {
      type: `count`
    },

    authorCount: {
      sql: `AUTHOR_USERNAME`,
      type: `countDistinct`
    },

    minCreatedAt: { sql: `CREATED_AT`, type: `min` },
    maxCreatedAt: { sql: `CREATED_AT`, type: `max` },

    minTextLength: { sql: `TEXT_LENGTH`, type: `min` },
    maxTextLength: { sql: `TEXT_LENGTH`, type: `max` },
    avgTextLength: { sql: `TEXT_LENGTH`, type: `avg` },

    minRetweetCount: { sql: `RETWEET_COUNT`, type: `min` },
    maxRetweetCount: { sql: `RETWEET_COUNT`, type: `max` },
    avgRetweetCount: { sql: `RETWEET_COUNT`, type: `avg` },

    minReplyCount: { sql: `REPLY_COUNT`, type: `min` },
    maxReplyCount: { sql: `REPLY_COUNT`, type: `max` },
    avgReplyCount: { sql: `REPLY_COUNT`, type: `avg` },

    minLikeCount: { sql: `LIKE_COUNT`, type: `min` },
    maxLikeCount: { sql: `LIKE_COUNT`, type: `max` },
    avgLikeCount: { sql: `LIKE_COUNT`, type: `avg` },

    minQuoteCount: { sql: `QUOTE_COUNT`, type: `min` },
    maxQuoteCount: { sql: `QUOTE_COUNT`, type: `max` },
    avgQuoteCount: { sql: `QUOTE_COUNT`, type: `avg` },

    minMentionCount: { sql: `MENTION_COUNT`, type: `min` },
    maxMentionCount: { sql: `MENTION_COUNT`, type: `max` },
    avgMentionCount: { sql: `MENTION_COUNT`, type: `avg` },

    minHashtagCount: { sql: `HASHTAG_COUNT`, type: `min` },
    maxHashtagCount: { sql: `HASHTAG_COUNT`, type: `max` },
    avgHashtagCount: { sql: `HASHTAG_COUNT`, type: `avg` },

    minLinkCount: { sql: `URL_COUNT`, type: `min` },
    maxLinkCount: { sql: `URL_COUNT`, type: `max` },
    avgLinkCount: { sql: `URL_COUNT`, type: `avg` },
  },
  
  dimensions: {
    id: {
      sql: `ID`,
      type: `number`,
      primaryKey: true,
      shown: true
    },

    createdAt: {
      sql: `CREATED_AT`,
      type: `number`
    },
    
    language: {
      sql: `LANGUAGE`,
      type: `string`
    },
    
    replySettings: {
      sql: `REPLY_SETTINGS`,
      type: `string`
    },
    
    source: {
      sql: `SOURCE`,
      type: `string`
    },

    text: {
      sql: `TEXT`,
      type: `string`
    },

    length: {
      sql: `TEXT_LENGTH`,
      type: `number`
    },

    retweets: {
      sql: `RETWEET_COUNT`,
      type: `number`
    },

    replies: {
      sql: `REPLY_COUNT`,
      type: `number`
    },

    likes: {
      sql: `LIKE_COUNT`,
      type: `number`
    },

    quoteTweets: {
      sql: `QUOTE_COUNT`,
      type: `number`
    },

    authorUsername: {
      sql: `AUTHOR_USERNAME`,
      type: `string`
    },

    authorName: {
      sql: `AUTHOR_NAME`,
      type: `string`
    },

    authorDescription: {
      sql: `AUTHOR_DESCRIPTION`,
      type: `string`
    },

    authorLocation: {
      sql: `AUTHOR_LOCATION`,
      type: `string`
    },

    authorVerified: {
      sql: `AUTHOR_VERIFIED`,
      type: `string`
    },

    mentions: {
      sql: `MENTION_COUNT`,
      type: `number`
    },

    hashtags: {
      sql: `HASHTAG_COUNT`,
      type: `number`
    },

    links: {
      sql: `URL_COUNT`,
      type: `number`
    },
  },

  preAggregations: {
    main: {
      type: `originalSql`,
      external: true,
      uniqueKeyColumns: [ '`ID`' ],
    },
  },
});
