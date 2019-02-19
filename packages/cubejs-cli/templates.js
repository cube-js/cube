const indexJs = `const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ port }) => {
  console.log(\`🚀 Cube.js server is listening on \${port}\`);
});
`;

const appJs = `const express = require('serverless-express/express');
const app = express();
app.use(require('cors')());
const serverCore = require('@cubejs-backend/server-core').create();

serverCore.initApp(app);

module.exports = app;
`;

const handlerJs = `const handler = require('serverless-express/handler');
const app = require('./app');

exports.api = handler(app);
`;

const dotEnv = (env) => `CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_DB_TYPE=${env.dbType}
CUBEJS_API_SECRET=${env.apiSecret}
`;

const serverlessYml = (env) => `service: ${env.projectName}

provider:
  name: aws
  runtime: nodejs8.10
  environment:
    CUBEJS_DB_HOST: <YOUR_DB_HOST_HERE>
    CUBEJS_DB_NAME: <YOUR_DB_NAME_HERE>
    CUBEJS_DB_USER: <YOUR_DB_USER_HERE>
    CUBEJS_DB_PASS: <YOUR_DB_PASS_HERE>
    CUBEJS_DB_PORT: <YOUR_DB_PORT_HERE>
    CUBEJS_DB_TYPE: ${env.dbType}
    CUBEJS_API_SECRET: ${env.apiSecret}
    REDIS_URL: <YOUR_REDIS_URL_HERE>
    CUBEJS_API_URL:
      Fn::Join:
      - ""
      - - "https://"
        - Ref: "ApiGatewayRestApi"
        - ".execute-api."
        - Ref: "AWS::Region"
        - ".amazonaws.com/\${self:provider.stage}"

functions:
  cubejs:
    handler: handler.api
    timeout: 30
#   vpc:
#     securityGroupIds:
#       - sg-12345678901234567 # Your DB and Redis security groups here
#     subnetIds:
#       - subnet-12345678901234567 # Your DB and Redis subnets here
    events:
      - http:
          path: /
          method: GET
      - http:
          path: /{proxy+}
          method: ANY

plugins:
  - serverless-express
`;

const ordersJs = `cube(\`Orders\`, {
  sql: \`
  select 1 as id, 100 as amount, 'new' status
  UNION ALL
  select 2 as id, 200 as amount, 'new' status
  UNION ALL
  select 3 as id, 300 as amount, 'processed' status
  UNION ALL
  select 4 as id, 500 as amount, 'processed' status
  UNION ALL
  select 5 as id, 600 as amount, 'shipped' status
  \`,

  measures: {
    count: {
      type: \`count\`
    },

    totalAmount: {
      sql: \`amount\`,
      type: \`sum\`
    }
  },

  dimensions: {
    status: {
      sql: \`status\`,
      type: \`string\`
    }
  }
});
`;

exports.express = {
  files: {
    'index.js': () => indexJs,
    '.env': dotEnv,
    'schema/Orders.js': () => ordersJs
  }
};

exports.serverless = {
  files: {
    'handler.js': () => handlerJs,
    'app.js': () => appJs,
    'serverless.yml': serverlessYml,
    '.env': dotEnv,
    'schema/Orders.js': () => ordersJs
  },
  dependencies: ['serverless-express']
};