cube(`Orders`, {
    sql: `SELECT * FROM public.orders`,

    measures: {
        count: {
            type: `count`,
            filters: [
                { sql: `${CUBE}.completed_at >= DATE('2019-08-01')` },
            ],
        },
        count2: {
            type: `count`,
            filters: [
                { sql: `${CUBE}.completed_at >= DATE('2019-08-01')` },
            ],
        },
    },

    dimensions: {
        id: {
            sql: `id`,
            type: `number`,
            primaryKey: true,
        },

        status: {
            sql: `status`,
            type: `string`,
        },

        completedAt: {
            sql: `completed_at`,
            type: `time`,
        },
    },

    preAggregations: {
        ordersByCompletedAt: {
            lambdaView: true,
            measures: [count, count2],
            timeDimension: completedAt,
            granularity: `day`,
            partitionGranularity: `week`,
            buildRangeStart: { sql: `SELECT DATE('2019-06-01')` },
            buildRangeEnd: { sql: `SELECT DATE('2019-09-01')` },
            refreshKey: {
                every: '1 minute',
                updateWindow: '3 week',
            },
        }
    },
});
