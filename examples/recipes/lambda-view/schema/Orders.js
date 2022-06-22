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
            measures: [count],
            timeDimension: completedAt,
            granularity: `day`,
            partitionGranularity: `month`,
            // buildRangeStart: {
            //     // sql: `SELECT NOW() - interval '365 day'`,
            //     sql: `SELECT DATE('2019-06-10')`,
            // },
            buildRangeEnd: {
                // sql: `SELECT NOW() - interval '3 day'`,
                sql: `SELECT DATE('2020-06-7')`,
            },
            refreshKey: {
                // every: '1 minute',
                every: '1 week',
            },
        },
    },
});
