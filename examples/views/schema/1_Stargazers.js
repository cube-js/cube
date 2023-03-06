cube(`Stargazers`, {
  // shown: false,

  // FYI: Cubes can be defined over arbitrary SQL statements
  sql: `
    SELECT *
    FROM airbyte.stargazers AS s
      LEFT JOIN airbyte.stargazers_user AS su
        ON su._airbyte_stargazers_hashid = s._airbyte_stargazers_hashid
  `,

  measures: {
    count: {
      sql: `login`,
      type: `count`
    },

    total: {
      sql: `login`,
      type: `count`,
      rollingWindow: {
        trailing: `unbounded`
      },
    },
  },
	
  dimensions: {
    repository: {
      sql: `repository`,
      type: `string`
    },

    starred_at: {
      sql: `starred_at`,
      type: `time`
    },

    login: {
      sql: `login`,
      type: `string`,
    },
    
    html_url: {
      sql: `html_url`,
      type: `string`,
    }
  }
});
