const statuses = ['completed'];

const createTotalByStatusMeasure = (status) => ({
    type: `count`,
    filters: [
      {
        sql: (CUBE) => `${CUBE}."status" = '${status}'`,
      },
    ],
});

const createPercentangeMeasure = (status) => ({
  type: `number`,
  format: `percent`,
  title: `Percentage for ${status}`,
  sql: (CUBE) => `${CUBE[`Total_${status}`]} / ${CUBE.totalOrders} * 100.0`,
});

cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: Object.assign(
    {
      totalOrders: {
        type: `count`
      },
    },
    statuses
      .map((status) => ({
        [`Total_${status}`]: createTotalByStatusMeasure(status),
        [`Percentage_for_${status}`]: createPercentangeMeasure(status),
      }))
      .reduce((status) => Object.assign(status))
  ),
});
