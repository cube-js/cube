cube(`Users`, {
  sql: `SELECT * FROM public.users`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [profileRealName, teamId, profileFirstName, profileDisplayNameNormalized, id, profileRealNameNormalized, name, profileDisplayName, profileTitle, profileLastName, realName]
    }
  },
  
  dimensions: {
    isRestricted: {
      sql: `is_restricted`,
      type: `string`
    },
    
    color: {
      sql: `color`,
      type: `string`
    },
    
    profileRealName: {
      sql: `${CUBE}."profile.real_name"`,
      type: `string`,
      title: `Profile.real Name`
    },
    
    profileStatusText: {
      sql: `${CUBE}."profile.status_text"`,
      type: `string`,
      title: `Profile.status Text`
    },
    
    isAdmin: {
      sql: `is_admin`,
      type: `string`
    },
    
    isOwner: {
      sql: `is_owner`,
      type: `string`
    },
    
    profileStatusEmoji: {
      sql: `${CUBE}."profile.status_emoji"`,
      type: `string`,
      title: `Profile.status Emoji`
    },
    
    profileImage72: {
      sql: `${CUBE}."profile.image_72"`,
      type: `string`,
      title: `Profile.image 72`
    },
    
    profileTeam: {
      sql: `${CUBE}."profile.team"`,
      type: `string`,
      title: `Profile.team`
    },
    
    profilePhone: {
      sql: `${CUBE}."profile.phone"`,
      type: `string`,
      title: `Profile.phone`
    },
    
    isPrimaryOwner: {
      sql: `is_primary_owner`,
      type: `string`
    },
    
    teamId: {
      sql: `team_id`,
      type: `string`
    },
    
    profileFirstName: {
      sql: `${CUBE}."profile.first_name"`,
      type: `string`,
      title: `Profile.first Name`
    },
    
    profileSkype: {
      sql: `${CUBE}."profile.skype"`,
      type: `string`,
      title: `Profile.skype`
    },
    
    profileFieldsXfc07yuz26Alt: {
      sql: `${CUBE}."profile.fields.XfC07YUZ26.alt"`,
      type: `string`,
      title: `Profile.fields.xfc07yuz26.alt`
    },
    
    deleted: {
      sql: `deleted`,
      type: `string`
    },
    
    profileDisplayNameNormalized: {
      sql: `${CUBE}."profile.display_name_normalized"`,
      type: `string`,
      title: `Profile.display Name Normalized`
    },
    
    profileAvatarHash: {
      sql: `${CUBE}."profile.avatar_hash"`,
      type: `string`,
      title: `Profile.avatar Hash`
    },
    
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true
    },
    
    profileRealNameNormalized: {
      sql: `${CUBE}."profile.real_name_normalized"`,
      type: `string`,
      title: `Profile.real Name Normalized`
    },
    
    name: {
      sql: `name`,
      type: `string`
    },
    
    isBot: {
      sql: `is_bot`,
      type: `string`
    },
    
    profileDisplayName: {
      sql: `${CUBE}."profile.display_name"`,
      type: `string`,
      title: `Profile.display Name`
    },
    
    profileImage24: {
      sql: `${CUBE}."profile.image_24"`,
      type: `string`,
      title: `Profile.image 24`
    },
    
    profileImageOriginal: {
      sql: `${CUBE}."profile.image_original"`,
      type: `string`,
      title: `Profile.image Original`
    },
    
    profileImage32: {
      sql: `${CUBE}."profile.image_32"`,
      type: `string`,
      title: `Profile.image 32`
    },
    
    profileImage48: {
      sql: `${CUBE}."profile.image_48"`,
      type: `string`,
      title: `Profile.image 48`
    },
    
    profileFieldsXfc07yuz26Value: {
      sql: `${CUBE}."profile.fields.XfC07YUZ26.value"`,
      type: `string`,
      title: `Profile.fields.xfc07yuz26.value`
    },
    
    profileIsCustomImage: {
      sql: `${CUBE}."profile.is_custom_image"`,
      type: `string`,
      title: `Profile.is Custom Image`
    },
    
    profileTitle: {
      sql: `${CUBE}."profile.title"`,
      type: `string`,
      title: `Profile.title`
    },
    
    profileEmail: {
      sql: `${CUBE}."profile.email"`,
      type: `string`,
      title: `Profile.email`
    },
    
    profileImage1024: {
      sql: `${CUBE}."profile.image_1024"`,
      type: `string`,
      title: `Profile.image 1024`
    },
    
    profileLastName: {
      sql: `${CUBE}."profile.last_name"`,
      type: `string`,
      title: `Profile.last Name`
    },
    
    isAppUser: {
      sql: `is_app_user`,
      type: `string`
    },
    
    tz: {
      sql: `tz`,
      type: `string`
    },
    
    profileImage192: {
      sql: `${CUBE}."profile.image_192"`,
      type: `string`,
      title: `Profile.image 192`
    },
    
    profileStatusTextCanonical: {
      sql: `${CUBE}."profile.status_text_canonical"`,
      type: `string`,
      title: `Profile.status Text Canonical`
    },
    
    tzLabel: {
      sql: `tz_label`,
      type: `string`
    },
    
    realName: {
      sql: `real_name`,
      type: `string`
    },
    
    profileImage512: {
      sql: `${CUBE}."profile.image_512"`,
      type: `string`,
      title: `Profile.image 512`
    },
    
    isUltraRestricted: {
      sql: `is_ultra_restricted`,
      type: `string`
    }

    updated: {
      sql: `updated`,
      type: `string`
    }
  }
});
