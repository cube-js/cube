export const yesNoCase = ({ sql }) => {
  return {
    type: `string`,
    case: {
      when: [ { sql: sql , label: `Yes` } ],
      else: { label: `No` }
    }
  }
}

 export const tiers = ({ field, tiers }) => {
   const whens = []
   tiers.forEach((tier, index) => {
     if (index === 0) {
       whens.push({ sql: () => `${field} <= ${tier}`, label: `0 - ${tier}`})  
     } else {
       whens.push(
         {
           sql: () => `${field} > ${tiers[index - 1]} AND ${field} <= ${tier}`,
           label: `${tiers[index - 1] + 1} - ${tier}`
         }
       )
     }
   });

   const lastTier = tiers[tiers.length - 1];
   whens.push({ sql: () => `${field} > ${lastTier}`, label: `${lastTier + 1}+`});

   return {
     type: `string`,
     case: {
       when: whens,
       else: { label: `Unknown` }
     }
   }
 }

cube(`Stories`, {
  sql: `
    SELECT *
    FROM \`bigquery-public-data.hacker_news.stories\`
    WHERE time_ts IS NOT NULL
  `,

  measures: {
    count: {
      type: `count`
    },

    authorsCount: {
      type: `count`,
      sql: `author`
    },

    showOrAskCount: {
      type: `count`,
      filters: [
        { sql: `${showOrAsk}` }
      ]
    },

    deadCount: {
      type: `count`,
      filters: [
        { sql: `${dead} = 'Yes'` }
      ]
    },

    percentageOfDead: {
      sql: `100.0 * ${deadCount} / NULLIF(${count}, 0)`,
      type: `number`
    },

    highRankedCount: {
      type: `count`,
      filters: [
        { sql: `${score} > 500` }
      ]
    },

    percentageOfHighRanked: {
      sql: `100.0 * ${highRankedCount} / NULLIF(${count}, 0)`,
      type: `number`
    },

    totalScore: {
      sql: `score`,
      type: `sum`
    },

    averageScore: {
      sql: `score`,
      type: `avg`
    }
  },

  dimensions: {
    id: {
      title: `ID`,
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    },

    time: {
      sql: `time_ts`,
      type: `time`
    },

    score: {
      sql: `score`,
      type: `number`
    },

    submitter: {
      sql: `by`,
      type: `string`
    },

    author: {
      sql: `author`,
      type: `string`
    },

    title: {
      sql: `title`,
      type: `string`
    },

    text: {
      sql: `text`,
      type: `string`
    },

    url: {
      sql: `url`,
      type: `string`,
      title: `URL`
    },

    descendants: {
      sql: `descendants`,
      type: `string`
    },

    category: {
      type: `string`,
      case: {
        when: [
          { sql: `STARTS_WITH(${title}, "Show HN")`, label: `Show HN` },
          { sql: `STARTS_WITH(${title}, "Ask HN")`, label: `Ask HN` }
        ],
        else: { label: `Other` }
      }
    },

    deleted: yesNoCase({ sql: `deleted` }),
    dead: yesNoCase({ sql: `dead` }),
    scoreTier: tiers({ field: `score`, tiers: [5, 10, 50, 100, 500] })
  },

  segments: {
    showOrAsk: {
      sql: `${category} != "Other"`
    }
  }
});
