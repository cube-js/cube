cube(`Orders`, {
  sql: `SELECT * FROM public.orders
    ${SECURITY_CONTEXT.role.unsafeValue() !== 'admin' ? 'WHERE id % 10 = FLOOR(RANDOM() * 10)' : ''}`,

  measures: {
    count: {
      type: `count`,
    },
  },
  
  dimensions: {
    status: {
      sql: `status`,
      type: `string`
    },
  },
  
  dataSource: `default`
});
