cube(`Reactions`, {
  sql: `SELECT * FROM public.reactions`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [clientMsgId]
    }
  },
  
  dimensions: {
    reactions: {
      sql: `reactions`,
      type: `string`
    },
    
    clientMsgId: {
      sql: `client_msg_id`,
      type: `string`
    }
  }
});
