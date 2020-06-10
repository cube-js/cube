cube(`pg__users`, {
    sql: `SELECT * FROM public.users`,
    dataSource: `mapbox__example`,
    joins: {
        mapbox: {
            sql: `${CUBE}.country = ${mapbox}.name`,
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
            type: `count`,
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

        location: {
            sql: `location`,
            type: 'string'
        },

        json: {
            sql: 'json',
            type: 'string'
        },

        country: {
            sql: 'country',
            type: 'string'
        }
    },
});
