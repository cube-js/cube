// Cube.js configuration options: https://cube.dev/docs/config

// Default data source credentials.
// See Settings -> Env vars to review or update them
const host     = process.env.CUBEJS_DB_HOST;
const port     = process.env.CUBEJS_DB_PORT;
const database = process.env.CUBEJS_DB_NAME;
const user     = process.env.CUBEJS_DB_USER;
const password = process.env.CUBEJS_DB_PASS;

const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  /* Part 1. Enforcing tenant-aware filters on all queries */
  /* ----------------------------------------------------- */

  // Apply a tenant-aware filter to all queries
  queryRewrite: (query, { securityContext }) => {
    // Ensure that the security context has the `merchant_id` property
    if (!securityContext.merchant_id) {
      throw new Error('No Merchant ID found in Security Context!');
    }

    // Apply a filter to all queries.  Cube will make sure to join
    // the `Merchants` cube to other cubes in a query to apply the filter
    query.filters.push({
      member: "Merchants.id",
      operator: "equals",
      values: [ securityContext.merchant_id ]
    });

    return query;
  },

  // Provide tenant-aware access to data sources

  // Now configure Cube to treat every tenant independently, by appId.
  // This is required for different database connections
  contextToAppId: ({ securityContext }) => `CUBEJS_APP_${securityContext.merchant_id}`,

  // We also must configure preAggregationsSchema to prevent preAggregation conflicts on the same table
  preAggregationsSchema: ({ securityContext }) => `pre_aggregations_${securityContext.merchant_id}`,

  // define driverFactory for the two databases
  driverFactory: ({ securityContext }) => {
    // Ensure that the security context has the `merchant_id` property
    if (!securityContext.merchant_id) {
      throw new Error('No Merchant ID found in Security Context!');
    }

    if (securityContext.merchant_id === 1) {
      return new PostgresDriver({
        // This tenant uses a separate database:
        database: 'multitenancy_workshop_aux',
        host,
        user,
        password,
        port,
      });
    } else {
      return new PostgresDriver({
        // All other tenants use a shared (default) database
        database,
        host,
        user,
        password,
        port,
      });
    }
  },

  /* Part 2. Static data schema with pre-aggregations */
  /* ------------------------------------------------ */

  // `scheduledRefreshContexts` return an array of security contexts.
  // Cube's refresh worker uses them to build pre-aggregations
/*  
  scheduledRefreshContexts: async () => [
    {
      securityContext: {
        merchant_id: 1,
      },
    },
    {
      securityContext: {
        merchant_id: 2,
      },
    },
  ],
*/
  /* Part 3. Dynamic tenant-aware data schema */
  /* ---------------------------------------- */

  // Fetch info on all available tenants from the database.
  // Provide it to Cube so it's able to refresh pre-aggregations
  scheduledRefreshContexts: async () => {
    const merchantIds = await fetchMerchants();

    return merchantIds.map((id) => {
      return { securityContext: { merchant_id: id } }
    })
  }
};

/* Part 3. Dynamic tenant-aware data schema */
/* ---------------------------------------- */
// NOTE: third-party dependencies and the use of require(...) are disabled for
// CubeCloud users by default.  Please contact support if you need them
// enabled for your account.  You are still allowed to require
// @cubejs-backend/*-driver packages.

const { Pool } = require('pg');

const pool = new Pool({
  host,
  port,
  user,
  password,
  database,
});

const merchantsQuery = `
  SELECT DISTINCT id
  FROM public.merchants
`;

const fetchMerchants = async () => {
  const client = await pool.connect();
  try {
    const result = await client.query(merchantsQuery);
    return result.rows.map((row) => row.id);
  } finally {
    client.release();
  }
};
