cube('BooleanFixture', {
  sql: `
    select 1 as id, 'a' as company_code, CAST(1 AS BIT) as completed
    UNION ALL
    select 2 as id, 'b' as company_code, CAST(0 AS BIT) as completed
    UNION ALL
    select 3 as id, 'b' as company_code, CAST(NULL AS BIT) as completed
    `,
  measures: {
    count: {
      type: 'count',
    },
  },
  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primaryKey: true,
      public: true,
    },
    companyCode: {
      sql: 'company_code',
      type: 'string',
    },
    completed: {
      sql: 'completed',
      type: 'boolean',
    },
  },
});
