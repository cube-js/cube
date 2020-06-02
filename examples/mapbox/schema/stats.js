cube(`stats`, {
  sql: `SELECT * FROM public.stats`,

  joins: {
    mapbox: {
      sql: `${CUBE}.countryterritorycode = ${mapbox}.iso_a3`,
      relationship: `belongsTo`,
    },
  },

  measures: {
    count: {
      sql: `id`,
      type: `count`,
    },

    total: {
      sql: `cases`,
      type: `sum`,
    },

    endDate: {
      sql: `${dateNum}`,
      type: 'max',
    },

    startDate: {
      sql: `${dateNum}`,
      type: 'min',
    },
  },

  dimensions: {
    date: {
      sql: `TO_TIMESTAMP(CONCAT_WS('-', ${CUBE}.year, ${CUBE}.month, ${CUBE}.day), 'YYYY-MM-DD')`,
      type: 'time',
    },

    /*date: {
      sql: `CONCAT_WS('-', ${CUBE}.year, ${CUBE}.month, ${CUBE}.day)`,
      type: 'time',
    },*/

    /* dateNum: {
      sql: `CONCAT_WS('-', ${CUBE}.year, ${CUBE}.month, ${CUBE}.day)`,
      type: 'time',
    },*/

    dateNum: {
      sql: `date_part('epoch',${date})`,
      type: 'number',
    },

    countryterritorycode: {
      sql: `countryterritorycode`,
      type: `string`,
    },

    id: {
      sql: `${CUBE}.date || '-' || ${CUBE}.countryterritorycode`,
      type: `string`,
      primaryKey: true,
    },
  },

  preAggregations: {
    amountByDate: {
      type: `rollup`,
      measureReferences: [total],
      timeDimensionReference: date,
      granularity: `day`,
    },
  },
});
