// Sales dollars, pre-aggregated to day/item/location in the warehouse. The
// numerator of AOV. West totals 100, East totals 60.
cube(`ItemLocationSales`, {
  sql: `
  select 1 as id, 1 as location_id, 30 as sales_amount
  UNION ALL
  select 2 as id, 1 as location_id, 70 as sales_amount
  UNION ALL
  select 3 as id, 2 as location_id, 60 as sales_amount
  `,

  joins: {
    Locations: {
      sql: `${CUBE}.location_id = ${Locations}.id`,
      relationship: `many_to_one`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
  },

  measures: {
    salesAmount: {
      sql: `sales_amount`,
      type: `sum`,
    },
  },
});
