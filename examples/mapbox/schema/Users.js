cube(`Users`, {
  sql: `SELECT * FROM public.Users WHERE geometry is not null`,
  joins: {
    Mapbox: {
      sql: `${CUBE}.country = ${Mapbox}.geounit`,
      relationship: `belongsTo`,
    },
  },
  measures: {
    total: {
      sql: `reputation`,
      type: `sum`,
    },

    avg: {
      sql: `reputation`,
      type: `avg`,
    },

    max: {
      sql: `reputation`,
      type: `max`,
    },

    min: {
      sql: `reputation`,
      type: `min`,
    },

    count: {
      type: `count`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: 'number',
      primaryKey: true,
      shown: true
    },

    value: {
      sql: `reputation`,
      type: 'number'

    },

    geometry: {
      sql: 'geometry',
      type: 'string'
    },

    country: {
      sql: 'country',
      type: 'string'
    }
  },

  preAggregations: {
    usersMeasures: {
      type: `rollup`,
      measureReferences: [Users.avg, Users.total],
      dimensionReferences: [MapboxCoords.coordinates, Users.country]
    },

    usersRating: {
      type: `rollup`,
      measureReferences: [Users.max],
      dimensionReferences: [Users.geometry, Users.value]
    }
  }
});
