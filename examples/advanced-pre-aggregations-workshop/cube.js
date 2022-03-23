/**
 * Step 9
 * Multi-tenancy
 */
 module.exports = {
  /**
   * Part 1
   * Enforcing tenant-aware filters on all queries
   */
  
  /* ----------------------------------------------------- */

  // Apply a tenant-aware filter to all queries
  queryRewrite: (query, { securityContext }) => {
    // Ensure that the security context has the `rRegionkey` property
    if (!securityContext.rRegionkey) {
      throw new Error('No Region Key found in Security Context!');
    }

    // Apply a filter to all queries.  Cube will make sure to join
    // the `Region` cube to other cubes in a query to apply the filter
    query.filters.push({
      member: "Region.rRegionkey",
      operator: "equals",
      values: [ securityContext.rRegionkey ]
    });

    return query;
  },

  /**
   * Part 2
   * Dynamic tenant-aware data schema
   * Pre-aggregations are separated by tenant id
   */
  
  // Provide tenant-aware access to data sources
  // Now configure Cube to treat every tenant independently, by appId.
  // This is required for different database connections
  contextToAppId: ({ securityContext }) => `CUBEJS_APP_${securityContext.rRegionkey}`,

  // We also must configure preAggregationsSchema to prevent preAggregation conflicts on the same table
  preAggregationsSchema: ({ securityContext }) => `pre_aggregations_${securityContext.rRegionkey}`,

  
  /* ---------------------------------------- */

  // Fetch info on all available tenants from the database.
  // Provide it to Cube so it's able to refresh pre-aggregations
  scheduledRefreshContexts: async () => {
    // Option 1.
    // const rRegionkeys = await fetchRegionKeys();
    // Option 2.
    const rRegionkeys = await fetchRegionKeysGCPFun();

    function mapSecurityContext() {
      return rRegionkeys.map(rRegionkey => {
        return { securityContext: { rRegionkey } }
      })
    }
    return mapSecurityContext();
  }
};

/** 
 * Part 2
 * Dynamic tenant-aware data schema
 * Pre-aggregations are separated by tenant id
 */

/* ---------------------------------------- */
// NOTE: third-party dependencies and the use of require(...) are disabled for
// CubeCloud users by default.  Please contact support if you need them
// enabled for your account.  You are still allowed to require
// @cubejs-backend/*-driver packages.

// const { BigQuery } = require('@google-cloud/bigquery');
// const bigquery = new BigQuery();
// async function fetchRegionKeys() {
//   const regionsQuery = `
//     SELECT DISTINCT R_REGIONKEY
//     FROM \`cube-devrel-team.tpc_h.region\`
//   `;

//   const options = {
//     query: regionsQuery,
//     // Location must match that of the dataset(s) referenced in the query.
//     location: 'US',
//   };

//   // Run the query as a job
//   const [ job ] = await bigquery.createQueryJob(options);
//   console.log(`Job ${ job.id } started.`);

//   // Wait for the query to finish
//   const [ rows ] = await job.getQueryResults();
//   const regionKeys = rows.map(row => row['R_REGIONKEY']);

//   // Return regionKeys
//   return regionKeys;
// };

/* ------------------------------------------ */
// NOTE: You can also use a GCP Function

const request = require('./utils/request');
async function fetchRegionKeysGCPFun() {
  const options = {
    host: 'us-central1-cube-devrel-team.cloudfunctions.net',
    path: '/fetchTpchRegions',
    port: '443'
  };
  const regionKeys = await request(options);
  return JSON.parse(regionKeys);
};
