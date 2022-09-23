cube(`Words`, {
  sql: 'SELECT * FROM WORDS',
  
  measures: {
    count: {
      type: `count`
    },

    minLength: { sql: `WORD_LENGTH`, type: `min` },
    maxLength: { sql: `WORD_LENGTH`, type: `max` },
    avgLength: { sql: `WORD_LENGTH`, type: `avg` },
  },
  
  dimensions: {
    tagId: {
      sql: `WORD_ID`,
      type: `string`,
      primaryKey: true,
      shown: true
    },
    
    word: {
      sql: `WORD`,
      type: `string`
    },
    
    length: {
      sql: `WORD_LENGTH`,
      type: `number`
    },
  },

  preAggregations: {
    main: {
      type: `originalSql`,
      external: true,
      uniqueKeyColumns: [ '`WORD_ID`' ],
    },
  },
});
