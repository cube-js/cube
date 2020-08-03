cube(`ChannelsMembers`, {
  sql: `SELECT * FROM public.channels_members`,
  
  joins: {
    Channels: {
      sql: `${CUBE}.channel_id = ${Channels}.id`,
      relationship: `belongsTo`
    }
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [memberId, channelId]
    }
  },
  
  dimensions: {
    memberId: {
      sql: `member_id`,
      type: `string`
    },
    
    channelId: {
      sql: `channel_id`,
      type: `string`
    }
  }
});
