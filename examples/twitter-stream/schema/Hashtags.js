cube(`Hashtags`, {
  sql: 'SELECT * FROM HASHTAGS',
  
  measures: {
    count: {
      type: `count`
    },
  },
  
  dimensions: {
    tagId: {
      sql: `TAG_ID`,
      type: `string`,
      primaryKey: true,
      shown: true
    },
    
    name: {
      sql: `TAG`,
      type: `string`
    },
  },

  preAggregations: {
    main: {
      type: `originalSql`,
      external: true,
      uniqueKeyColumns: [ '`TAG_ID`' ],
    },
  },
});
