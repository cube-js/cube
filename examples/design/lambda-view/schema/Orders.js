cube(`Orders`, {
    sql: `SELECT * FROM public.orders`,

    measures: {
        count: {
            type: `count`,
            drillMembers: [id, createdAt],
        },
    },

    dimensions: {
        status: {
            sql: `status`,
            type: `string`,
        },

        id: {
            sql: `id`,
            type: `number`,
            primaryKey: true,
        },

        completedAt: {
            sql: `completed_at`,
            type: `time`,
        },
    },

    preAggregations: {
        ordersByStatus: {
            measures: [count],
            dimensions: [status],
        },

        ordersByCompletedAt: {
            measures: [count],
            timeDimension: completedAt,
            granularity: `month`,
        },
    },
});
