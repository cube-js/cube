cube(`Orders`, {
    sql: `SELECT * FROM public.orders`,

    measures: {
        count: {
            type: `count`,
            filters: [
                // Last partition
                { sql: `${CUBE}.completed_at >= DATE('2020-01-01')` },
            ],
        },
        count2: {
            type: `count`,
            filters: [
                // Last partition
                { sql: `${CUBE}.completed_at >= DATE('2020-01-02')` },
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
        // // query: {
        // //     measures: [count],
        // //     dimensions: [status],
        // // }
        // ordersByStatus: {
        //     // lambdaView: true,
        //     measures: [count],
        //     dimensions: [status],
        //     // lambdaViewTimeDimension: completedAt,
        //     granularity: `month`
        // },
        //
        // // query: {
        // //     measures: [count],
        // //     dimensions: [status],
        // // }
        // ordersByStatus2: {
        //     // lambdaView: true,
        //     measures: [count],
        //     dimensions: [status],
        //     timeDimension: completedAt,
        //     granularity: `month`,
        // },
        //
        // // query: {
        // //     measures: [count],
        // //     timeDimension: completedAt,
        // // }
        // ordersByCompletedAt: {
        //     // lambdaView: true,
        //     measures: [count],
        //     timeDimension: completedAt,
        //     granularity: `month`,
        // },

        ordersByCompletedAtBuildRange: {
            lambdaView: true,
            measures: [count, count2],
            timeDimension: completedAt,
            granularity: `day`,
            partitionGranularity: `year`,
            buildRangeStart: { sql: `SELECT DATE('2019-07-01')` },
            // buildRangeEnd: { sql: `SELECT DATE('2021-07-01')` },
            refreshKey: {
                every: '1 hour',
            },
        }
    },
});
