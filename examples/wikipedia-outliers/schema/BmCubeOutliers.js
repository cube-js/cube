cube(`Outliers`, {
  sql: `
    SELECT
      *,
      UPPER(SPLIT_PART(wiki_code, '.', 1)) AS region,
      CONCAT('https://', wiki_code, '.org/wiki/', article_title) AS url
    FROM dev.bm_cube_outliers
  `,
  
  measures: {
    count: {
      type: `count`
    },
    
    dailyTotal: {
      sql: `daily_total`,
      type: `sum`
    }
  },
  
  dimensions: {
    wikiCode: {
      sql: `wiki_code`,
      type: `string`
    },

    region: {
      sql: `region`,
      type: `string`
    },

    url: {
      sql: `url`,
      type: `string`
    },
    
    title: {
      sql: `article_title`,
      type: `string`
    },
    
    average: {
      sql: `average`,
      type: `string`
    },
    
    stDev: {
      sql: `st_dev`,
      type: `string`
    },
    
    logDate: {
      sql: `log_date`,
      type: `time`
    }
  },

  segments: {
    wikipedia: {
      sql: `wiki_code LIKE '__.wikipedia'`
    },
    wikibooks: {
      sql: `wiki_code LIKE '__.wikibooks'`
    },
    wiktionary: {
      sql: `wiki_code LIKE '__.wiktionary'`
    },
    wikimedia: {
      sql: `wiki_code LIKE '__.wikimedia'`
    },
    wikiquote: {
      sql: `wiki_code LIKE '__.wikiquote'`
    }
  },

  preAggregations: {
    regions: {
      measures: [ count ],
      dimensions: [ region ],
      segments: [ wikipedia ],
      refreshKey: {
        every: '1 day'
      }
    },

    outliers: {
      measures: [ dailyTotal ],
      dimensions: [ region ],
      segments: [ wikipedia ],
      timeDimension: logDate,
      granularity: `day`,
      refreshKey: {
        every: '1 day'
      }
    }
  }
});
