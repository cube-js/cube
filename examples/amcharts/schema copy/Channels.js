cube(`Channels`, {
  sql: `SELECT * FROM public.channels`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [name, id]
    }
  },
  
  dimensions: {
    isGeneral: {
      sql: `is_general`,
      type: `string`
    },
    
    isArchived: {
      sql: `is_archived`,
      type: `string`
    },
    
    name: {
      sql: `name`,
      type: `string`
    },
    
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true
    },
    
    creator: {
      sql: `creator`,
      type: `string`
    }
  }
});
