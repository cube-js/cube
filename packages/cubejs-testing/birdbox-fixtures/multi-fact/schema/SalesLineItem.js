// Transaction lines - a finer grain than ItemLocationSales, and no join
// between the two. The denominator of AOV counts distinct transactions, so
// West's three lines on T100 must still count as one.
//
// West: T100 (3 lines) + T101 (1 line)          -> 2 transactions
// East: T200 counts, T201 is an EXCHANGE        -> 1 transaction
cube(`SalesLineItem`, {
  sql: `
  select 1 as id, 100 as transaction_id, 1 as location_id, 'SALE' as transaction_type, 'IN_STORE' as fulfillment_channel_group
  UNION ALL
  select 2 as id, 100 as transaction_id, 1 as location_id, 'SALE' as transaction_type, 'IN_STORE' as fulfillment_channel_group
  UNION ALL
  select 3 as id, 100 as transaction_id, 1 as location_id, 'SALE' as transaction_type, 'IN_STORE' as fulfillment_channel_group
  UNION ALL
  select 4 as id, 101 as transaction_id, 1 as location_id, 'SALE' as transaction_type, 'IN_STORE' as fulfillment_channel_group
  UNION ALL
  select 5 as id, 200 as transaction_id, 2 as location_id, 'SALE' as transaction_type, 'IN_STORE' as fulfillment_channel_group
  UNION ALL
  select 6 as id, 201 as transaction_id, 2 as location_id, 'EXCHANGE' as transaction_type, 'IN_STORE' as fulfillment_channel_group
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
    // The filter logic lives here, on the cube that owns the columns, so every
    // consumer picks it up by including the measure.
    transactionsWithoutReturns: {
      sql: `transaction_id`,
      type: `countDistinct`,
      filters: [
        { sql: `${CUBE}.transaction_type <> 'EXCHANGE'` },
        { sql: `${CUBE}.fulfillment_channel_group IN ('IN_STORE', 'SHIP_FROM_STORE')` },
      ],
    },

    // AOV owned by this cube, referencing the other fact's measure.
    aovBasket: {
      sql: `${ItemLocationSales.salesAmount} / NULLIF(${CUBE.transactionsWithoutReturns}, 0)`,
      type: `number`,
      multiStage: true,
    },
  },
});
