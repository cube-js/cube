/* Part 2. Static data schema with pre-aggregations */
/* ------------------------------------------------ */

// Extract the merchant id from the security context.
// Will be provided via COMPILE_CONTEXT from `scheduledRefreshContexts`.
// "Compile" means that it's available when the schema compilation happens:
// - on the first request to an API instance
// - on the pre-aggregations build performed by a refresh worker
const {
  securityContext: { merchant_id },
} = COMPILE_CONTEXT;

// Prepare a list of custom measures according to the tenant id
const customMeasures = {};

if (parseInt(merchant_id, 10) === 1) {
  // Only this tenant has orders with the "processing" status.
  // Add a custom measure for this tenant
  customMeasures[`processingCount`] = {
    type: `count`,
    filters: [
      { sql: (CUBE) => `${CUBE}.status = 'processing'` }
    ]
  }
};

cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  

    mainRollup: {
      measures: [ Orders.count ],
      dimensions: [ Merchants.id ],
      timeDimension: Orders.createdAt,
      granularity: `day`,
      refreshKey: {
        every: `1 minute`
      }
    }
  },

  joins: {
    Products: {
      sql: `${CUBE}.product_id = ${Products}.id`,
      relationship: `belongsTo`
    }
  },

  // Combine the list of custom measures with the static ones
  measures: Object.assign(
    customMeasures,
    {
      count: {
        type: `count`,
        drillMembers: (CUBE) => [`${CUBE}.id`, `${CUBE}.createdAt`]
      },
      number: {
        sql: (CUBE) => `number`,
        type: `sum`
      }
    }
  ),

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`
    },
    createdAt: {
      sql: `created_at`,
      type: `time`
    },
    completedAt: {
      sql: `completed_at`,
      type: `time`
    }
  }
});
