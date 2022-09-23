cube(`Mentions`, {
  sql: 'SELECT * FROM MENTIONS',
  
  measures: {
    count: {
      type: `count`
    },
  },
  
  dimensions: {
    tagId: {
      sql: `MENTION_ID`,
      type: `string`,
      primaryKey: true,
      shown: true
    },
    
    userName: {
      sql: `USERNAME`,
      type: `string`
    },
    
    userId: {
      sql: `USER_ID`,
      type: `string`
    },
  },

  preAggregations: {
    main: {
      type: `originalSql`,
      external: true,
      uniqueKeyColumns: [ '`MENTION_ID`' ],
    },
  },
});
