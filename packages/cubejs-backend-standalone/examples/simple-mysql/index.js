const express = require('express');
const app = express();
const bodyParser = require('body-parser');
app.use(bodyParser.json({ limit: '50mb' }));

const CubejsStandalone = require('@cubejs-backend/standalone');
const JDBCDriver = require('@cubejs-backend/jdbc-driver');

const cubejsStandalone = CubejsStandalone.create({
  driverFactory: () => new JDBCDriver({
    url: `jdbc:mysql://${process.env.DB_HOST}:3306/${process.env.DB_NAME}`,
    drivername: "com.mysql.jdbc.Driver",
    properties: {
      user: process.env.DB_USER,
      password: process.env.DB_PASS
    },
    prepareConnectionQueries: [`SET time_zone = '+00:00'`]
  }),
  apiSecret: 'YOUR_API_SECRET',
  dbType: 'mysql'
});

cubejsStandalone.initApp(app)
  .then(() => {
    const port = process.env.PORT || 6020;
    app.listen(port, () => {
      console.log(`Cube.js standalone backend listening on ${port}`);
    });
  }).catch(e => {
    console.error(e.stack || e);
  });