cube(`Products`, {
  sql: `SELECT * FROM public.products`,
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [id, name, createdAt]
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    name: {
      sql: `name`,
      type: `string`
    },
    
    description: {
      sql: `description`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },
  
  dataSource: `default`
});