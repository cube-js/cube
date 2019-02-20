const indexJs = `const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ port }) => {
  console.log(\`🚀 Cube.js server is listening on \${port}\`);
});
`;

const handlerJs = `module.exports = require('@cubejs-backend/serverless');
`;

// Shared environment variables, across all DB types
const sharedDotEnvVars = env => `CUBEJS_DB_TYPE=${env.dbType}
CUBEJS_API_SECRET=${env.apiSecret}`;

const athenaDotEnvVars = env => `CUBEJS_AWS_KEY=<YOUR ATHENA AWS KEY HERE>
CUBEJS_AWS_SECRET=<YOUR ATHENA SECRET KEY HERE>
CUBEJS_AWS_REGION=<AWS REGION STRING, e.g. us-east-1>
# You can find the Athena S3 Output location here: https://docs.aws.amazon.com/athena/latest/ug/querying.html
CUBEJS_AWS_S3_OUTPUT_LOCATION=<S3 OUTPUT LOCATION>
CUBEJS_JDBC_DRIVER=athena
${sharedDotEnvVars(env)}`;

const defaultDotEnvVars = env => `CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
${sharedDotEnvVars(env)}`;

const dotEnv = env => env.dbType === 'athena' ? athenaDotEnvVars(env) : defaultDotEnvVars(env);

const serverlessYml = env => `service: ${env.projectName}

provider:
  name: aws
  runtime: nodejs8.10
  iamRoleStatements:
    - Effect: "Allow"
      Action:
        - "sns:*"
# Athena permissions        
#        - "athena:*"
#        - "s3:*"
#        - "glue:*"
      Resource:
        - "*"
# When you uncomment vpc please make sure lambda has access to internet: https://medium.com/@philippholly/aws-lambda-enable-outgoing-internet-access-within-vpc-8dd250e11e12  
#  vpc:
#    securityGroupIds:
#     - sg-12345678901234567 # Your DB and Redis security groups here
#    subnetIds:
#     - subnet-12345678901234567 # Your DB and Redis subnets here
  environment:
    CUBEJS_DB_HOST: <YOUR_DB_HOST_HERE>
    CUBEJS_DB_NAME: <YOUR_DB_NAME_HERE>
    CUBEJS_DB_USER: <YOUR_DB_USER_HERE>
    CUBEJS_DB_PASS: <YOUR_DB_PASS_HERE>
    CUBEJS_DB_PORT: <YOUR_DB_PORT_HERE>
    REDIS_URL: <YOUR_REDIS_URL_HERE>
    CUBEJS_DB_TYPE: ${env.dbType}
    CUBEJS_API_SECRET: ${env.apiSecret}
    CUBEJS_APP: ${env.projectName}
    CUBEJS_API_URL:
      Fn::Join:
        - ""
        - - "https://"
          - Ref: "ApiGatewayRestApi"
          - ".execute-api."
          - Ref: "AWS::Region"
          - ".amazonaws.com/\${self:provider.stage}"
    AWS_ACCOUNT_ID:
      Fn::Join:
        - ""
        - - Ref: "AWS::AccountId"

functions:
  cubejs:
    handler: cube.api
    timeout: 30
    events:
      - http:
          path: /
          method: GET
      - http:
          path: /{proxy+}
          method: ANY
  cubejsProcess:
    handler: cube.process
    timeout: 630
    events:
      - sns: ${env.projectName}-process

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
    'cube.js': () => handlerJs,
    'serverless.yml': serverlessYml,
    '.env': dotEnv,
    'schema/Orders.js': () => ordersJs
  },
  dependencies: ['@cubejs-backend/serverless']
};

module.exports = {
  dotEnv,
}