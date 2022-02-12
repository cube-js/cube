const Stories = require("./Stories");

cube(`StoriesPreAgg`, {
  extends: Stories,

  preAggregations: {
    avgScByDay: {
      measures: [ averageScore ],
      dimensions: [ category ],
      granularity: `day`,
      timeDimension: time
    },
    avgScByWeek: {
      measures: [ averageScore ],
      dimensions: [ category ],
      granularity: `week`,
      timeDimension: time
    },
    main: {
      measures: [ count ],
      dimensions: [ category, dead ],
      granularity: `day`,
      timeDimension: time
    },
    authors: {
      measures: [ authorsCount ],
      dimensions: [ category ],
      granularity: `day`,
      timeDimension: time
    },
    prcOfHihRkd: {
      measures: [ count, highRankedCount ],
      dimensions: [ category ],
      granularity: `month`,
      timeDimension: time
    },
    countByTiers: {
      measures: [ count ],
      dimensions: [ category, scoreTier ],
      granularity: `day`,
      timeDimension: time
    }
  }
});
