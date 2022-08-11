cube(`Orders`, {
    sql: `SELECT * FROM public.orders`,

    measures: {
        count: {
            type: `count`,
        },

        count2: {
            type: `count`,
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
            unionWithSourceData: true,
            measures: [count, count2],
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
        },
    },
});
