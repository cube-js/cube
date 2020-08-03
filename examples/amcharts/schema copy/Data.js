cube(`Data`, {
  sql: `SELECT * FROM public.data`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [clientMsgId, userProfileFirstName, userProfileRealName, userProfileDisplayName, userProfileName]
    }
  },
  
  dimensions: {
    userProfileIsRestricted: {
      sql: `${CUBE}."user_profile.is_restricted"`,
      type: `string`,
      title: `User Profile.is Restricted`
    },
    
    userProfileAvatarHash: {
      sql: `${CUBE}."user_profile.avatar_hash"`,
      type: `string`,
      title: `User Profile.avatar Hash`
    },
    
    userProfileTeam: {
      sql: `${CUBE}."user_profile.team"`,
      type: `string`,
      title: `User Profile.team`
    },
    
    displayAsBot: {
      sql: `display_as_bot`,
      type: `string`
    },
    
    clientMsgId: {
      sql: `client_msg_id`,
      type: `string`
    },
    
    userTeam: {
      sql: `user_team`,
      type: `string`
    },
    
    team: {
      sql: `team`,
      type: `string`
    },
    
    userProfileImage72: {
      sql: `${CUBE}."user_profile.image_72"`,
      type: `string`,
      title: `User Profile.image 72`
    },
    
    subtype: {
      sql: `subtype`,
      type: `string`
    },
    
    userProfileFirstName: {
      sql: `${CUBE}."user_profile.first_name"`,
      type: `string`,
      title: `User Profile.first Name`
    },
    
    userProfileIsUltraRestricted: {
      sql: `${CUBE}."user_profile.is_ultra_restricted"`,
      type: `string`,
      title: `User Profile.is Ultra Restricted`
    },
    
    editedUser: {
      sql: `${CUBE}."edited.user"`,
      type: `string`,
      title: `Edited.user`
    },
    
    text: {
      sql: `text`,
      type: `string`
    },
    
    userProfileRealName: {
      sql: `${CUBE}."user_profile.real_name"`,
      type: `string`,
      title: `User Profile.real Name`
    },
    
    userProfileDisplayName: {
      sql: `${CUBE}."user_profile.display_name"`,
      type: `string`,
      title: `User Profile.display Name`
    },
    
    sourceTeam: {
      sql: `source_team`,
      type: `string`
    },
    
    user: {
      sql: `user`,
      type: `string`
    },
    
    userProfileName: {
      sql: `${CUBE}."user_profile.name"`,
      type: `string`,
      title: `User Profile.name`
    },
    
    type: {
      sql: `type`,
      type: `string`
    },
    
    topic: {
      sql: `topic`,
      type: `string`
    }
  }
});
