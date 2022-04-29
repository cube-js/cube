// Cube.js configuration options: https://cube.dev/docs/config

// NOTE: third-party dependencies and the use of require(...) are disabled for
// CubeCloud users by default.  Please contact support if you need them
// enabled for your account.  You are still allowed to require
// @cubejs-backend/*-driver packages.

module.exports = {};

/* ---------------------------------------- */

// /**
//  * Demo 4
//  * Multitenancy
//  */
// // Default data source credentials.
// // See Settings -> Env vars to review or update them
// const host     = process.env.CUBEJS_DB_HOST;
// const port     = process.env.CUBEJS_DB_PORT;
// const database = process.env.CUBEJS_DB_NAME;
// const user     = process.env.CUBEJS_DB_USER;
// const password = process.env.CUBEJS_DB_PASS;
// const PostgresDriver = require('@cubejs-backend/postgres-driver');

// function generateSupplierIds() {
//   // This can be an API request to get a dynamic list from the DB
//   // Take a look at our previous workshop where this was explained
//   // https://youtu.be/JxedV_zI7W4?t=5018
//   return [ ...Array(100).keys() ].map(i => i+1);
// }

// module.exports = {
// 	queryRewrite: (query, { securityContext }) => {
//     // Ensure that the security context has the `supplierId` property
//     if (!securityContext.supplierId) {
//       throw new Error('No Supplier ID found in Security Context!');
//     }

//     // Apply a filter to all queries.  Cube will make sure to join
//     // the `Suppliers` cube to other cubes in a query to apply the filter
//     query.filters.push({
//       member: "Suppliers.id",
//       operator: "equals",
//       values: [ securityContext.supplierId ]
//     });

//     return query;
//   },	

//   // Provide tenant-aware access to data sources
//   // Now configure Cube to treat every tenant independently, by appId.
//   // This is required for different database connections
//   contextToAppId: ({ securityContext }) => `CUBEJS_APP_${securityContext.supplierId}`,

//   driverFactory: ({ securityContext }) => {
//     // Ensure that the security context has the `supplierId` property
//     if (!securityContext.supplierId) {
//       throw new Error('No Supplier ID found in Security Context!');
//     }

//     if (securityContext.supplierId === 1) {
//       return new PostgresDriver({
//         // This tenant uses the ecom DB on GCP
//         database: 'ecom',
//         host: 'demo-db.cube.dev',
//         user,
//         password,
//         port: '5432',
//       });
//     } else {
//       return new PostgresDriver({
//         // All other tenants use Supabase
//         database,
//         host,
//         user,
//         password,
//         port,
//       });
//     }
//   },

//   /**
//    * Demo 5
//    * Pre-aggregations
//    */
//   // We also must configure preAggregationsSchema to prevent preAggregation conflicts on the same table
//   preAggregationsSchema: ({ securityContext }) => `pre_aggregations_${securityContext.supplierId}`,

//   // Provide static tenants to Cube so it's able to refresh pre-aggregations
//   scheduledRefreshContexts: () => {
//     // Generates an array of ids from 1 to 100
//     const supplierIds = generateSupplierIds();
//     function mapSecurityContext() {
//       return supplierIds.map(supplierId => {
//         return { securityContext: { supplierId } }
//       })
//     }
//     return mapSecurityContext();
//   },
// };
