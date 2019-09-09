const Stories = require("./Stories");

cube(`StoriesPreAgg`, {
  extends: Stories,
  preAggregations: {
    avgScByDay: {
      type: `rollup`,
      measureReferences: [averageScore],
      dimensionReferences: [category],
      granularity: `day`,
      timeDimensionReference: time,
      external: true
    },
    avgScByWeek: {
      type: `rollup`,
      measureReferences: [averageScore],
      dimensionReferences: [category],
      granularity: `week`,
      timeDimensionReference: time,
      external: true
    },
    main: {
      type: `rollup`,
      measureReferences: [count],
      dimensionReferences: [category, dead],
      granularity: `day`,
      timeDimensionReference: time,
      external: true
    },
    authors: {
      type: `rollup`,
      measureReferences: [authorsCount],
      dimensionReferences: [category],
      granularity: `day`,
      timeDimensionReference: time,
      external: true
    },
    prcOfHihRkd: {
      type: `rollup`,
      measureReferences: [count, highRankedCount],
      dimensionReferences: [category],
      granularity: `month`,
      timeDimensionReference: time,
      external: true
    },
    countByTiers: {
      type: `rollup`,
      measureReferences: [count],
      dimensionReferences: [category, scoreTier],
      granularity: `day`,
      timeDimensionReference: time,
      external: true
    }
  }
});
