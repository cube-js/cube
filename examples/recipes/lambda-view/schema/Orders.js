cube(`Orders`, {
    sql: `SELECT * FROM public.orders`,

    measures: {
        count: {
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
            dimensions: [status],
            timeDimension: completedAt,
            granularity: `day`,
            partitionGranularity: `month`,
            buildRangeStart: {
                sql: `SELECT DATE('2020-02-7')`,
            },
            buildRangeEnd: {
                sql: `SELECT DATE('2020-06-7')`,
            },
            refreshKey: {
                every: 'never',
            },
        },
    },
});
