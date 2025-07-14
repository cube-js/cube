cube(`custom_calendar_js`, {
  sql: `SELECT * FROM public.custom_calendar`,

  calendar: true,

  measures: {
    count: {
      type: `count`,
    }
  },

  dimensions: {
    date_val: {
      sql: `date_val`,
      type: `time`,
      primaryKey: true,
      shown:true
    },

    retail_date: {
      sql: `retail_date`,
      type: `time`,
      granularities: {
        week: {
          sql: `{CUBE.retail_week_begin_date}`,
        },
        month: {
          sql: `{CUBE.retail_month_begin_date}`,
        },
        quarter: {
          sql: `{CUBE.retail_quarter_year}`,
        },
        year: {
          sql: `{CUBE.retail_year_begin_date}`,
        }
      },
      timeShift: [
          {
              interval: '1 month',
              type: 'prior',
              sql: `{CUBE.retail_date_prev_month}`,
          },
          {
              name: 'retail_date_prev_year',
              sql: `{CUBE.retail_date_prev_year}`,
          },
          {
              interval: '2 year',
              type: 'prior',
              sql: `{CUBE.retail_date_prev_prev_year}`,
          }
      ]
    },

    retail_year: {
      sql: `retail_year_name`,
      type: `string`
    },

    retail_month_long_name: {
      sql: `retail_month_long_name`,
      type: `string`
    },

    retail_week_name: {
      sql: `retail_week_name`,
      type: `string`
    },

    retail_year_begin_date: {
      sql: `retail_year_begin_date`,
      type: `time`
    },

    retail_year_end_date: {
      sql: `retail_year_end_date`,
      type: `time`
    },

    retail_quarter_year: {
      sql: `retail_quarter_year`,
      type: `string`
    },

    retail_month_begin_date: {
      sql: `retail_month_begin_date`,
      type: `string`
    },

    retail_week_begin_date: {
      sql: `retail_week_begin_date`,
      type: `string`
    },

    retail_year_week: {
      sql: `retail_year_week`,
      type: `string`
    },

    retail_week_in_month: {
      sql: `retail_week_in_month`,
      type: `string`
    },

    retail_date_prev_month: {
      sql: `retail_date_prev_month`,
      type: `string`
    },

    retail_date_prev_year: {
      sql: `retail_date_prev_year`,
      type: `string`
    },

    retail_date_prev_prev_year: {
      sql: `retail_date_prev_prev_year`,
      type: `string`
    },

    fiscal_year: {
      sql: `fiscal_year`,
      type: `string`
    },

    fiscal_quarter_year: {
      sql: `fiscal_quarter_year`,
      type: `string`
    },

    fiscal_year_month_number: {
      sql: `fiscal_year_month_number`,
      type: `number`
    },

    fiscal_year_month_name: {
      sql: `fiscal_year_month_name`,
      type: `string`
    },

    fiscal_year_period_name: {
      sql: `fiscal_year_period_name`,
      type: `number`
    },

    fiscal_month_number: {
      sql: `fiscal_month_number`,
      type: `string`
    },

    fiscal_month_short_name: {
      sql: `fiscal_month_short_name`,
      type: `string`
    },

    fiscal_week_name: {
      sql: `fiscal_week_name`,
      type: `string`
    },

    fiscal_week_begin_date: {
      sql: `fiscal_week_begin_date`,
      type: `time`
    },

    fiscal_week_end_date: {
      sql: `fiscal_week_end_date`,
      type: `time`
    },

  }
});
