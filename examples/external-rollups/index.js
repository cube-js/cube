const CubejsServerCore = require("@cubejs-backend/server-core");
const MySQLDriver = require('@cubejs-backend/mysql-driver');
const express = require('express')
const app = express()
const path = require('path');
require('dotenv').config();

if (process.env.NODE_ENV === 'production') {
  app.use('/', express.static(path.join(__dirname, 'dashboard-app', 'build')))
} else {
  app.use(require('cors')());
}

const CubejsServer = new CubejsServerCore({
  preAggregationsSchema: () => process.env.CUBEJS_PREAGGREGATIONS_SCHEMA || 'stb_pre_aggregations',
  externalDbType: 'mysql',
  externalDriverFactory: () => new MySQLDriver({
    host: process.env.CUBEJS_EXT_DB_HOST,
    database: process.env.CUBEJS_EXT_DB_NAME,
    user: process.env.CUBEJS_EXT_DB_USER,
    password: process.env.CUBEJS_EXT_DB_PASS.toString()
  })
});

CubejsServer.initApp(app)

app.listen(process.env.PORT || 4000, () => console.log('App is up and running!'))
