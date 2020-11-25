const indexJs = `const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ version, port }) => {
  console.log(\`🚀 Cube.js server (\${version}) is listening on \${port}\`);
}).catch(e => {
  console.error('Fatal error during server start: ');
  console.error(e.stack || e);
});
`;

const handlerJs = `module.exports = require('@cubejs-backend/serverless');
`;

// Shared environment variables, across all DB types
const sharedDotEnvVars = env => `CUBEJS_DEV_MODE=true
CUBEJS_DB_TYPE=${env.dbType}
CUBEJS_API_SECRET=${env.apiSecret}`;

const defaultDotEnvVars = env => `# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables
CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_WEB_SOCKETS=true
${sharedDotEnvVars(env)}`;

const athenaDotEnvVars = env => `# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables
CUBEJS_AWS_KEY=<YOUR ATHENA AWS KEY HERE>
CUBEJS_AWS_SECRET=<YOUR ATHENA SECRET KEY HERE>
CUBEJS_AWS_REGION=<AWS REGION STRING, e.g. us-east-1>
# You can find the Athena S3 Output location here: https://docs.aws.amazon.com/athena/latest/ug/querying.html
CUBEJS_AWS_S3_OUTPUT_LOCATION=<S3 OUTPUT LOCATION>
CUBEJS_JDBC_DRIVER=athena
${sharedDotEnvVars(env)}`;

const mongobiDotEnvVars = env => `${defaultDotEnvVars(env)}
#CUBEJS_DB_SSL=<SSL_PROFILE>
#CUBEJS_DB_SSL_CA=<SSL_CA>
#CUBEJS_DB_SSL_CERT=<SSL_CERT>
#CUBEJS_DB_SSL_CIPHERS=<SSL_CIPHERS>
#CUBEJS_DB_SSL_PASSPHRASE=<SSL_PASSPHRASE>
#CUBEJS_DB_SSL_REJECT_UNAUTHORIZED=<SSL_REJECT_UNAUTHORIZED>`;

const dotEnv = env => {
  if (env.driverEnvVariables) {
    const envVars = env.driverEnvVariables.map(v => `${v}=<${v.replace('CUBEJS', 'YOUR')}>`).join('\n');
    return `${envVars}\n${sharedDotEnvVars(env)}`;
  }

  return {
    athena: athenaDotEnvVars(env),
    mongobi: mongobiDotEnvVars(env)
  }[env.dbType] || defaultDotEnvVars(env);
};

const gitIgnore = `.env
node_modules
`;

const serverlessYml = env => `service: ${env.projectName}

provider:
  name: aws
  runtime: nodejs12.x
  iamRoleStatements:
    - Effect: "Allow"
      Action:
        - "sns:*"
# Athena permissions
#        - "athena:*"
#        - "s3:*"
#        - "glue:*"
      Resource: '*'
# When you uncomment vpc please make sure lambda has access to internet: https://medium.com/@philippholly/aws-lambda-enable-outgoing-internet-access-within-vpc-8dd250e11e12
#  vpc:
#    securityGroupIds:
#     - sg-12345678901234567 # Your DB and Redis security groups here
#    subnetIds:
#     - subnet-12345678901234567 # Put here subnet with access to your DB, Redis and internet. For internet access 0.0.0.0/0 should be routed through NAT only for this subnet!
  environment:
    CUBEJS_DB_HOST: <YOUR_DB_HOST_HERE>
    CUBEJS_DB_NAME: <YOUR_DB_NAME_HERE>
    CUBEJS_DB_USER: <YOUR_DB_USER_HERE>
    CUBEJS_DB_PASS: <YOUR_DB_PASS_HERE>
    CUBEJS_DB_PORT: <YOUR_DB_PORT_HERE>
    REDIS_URL: <YOUR_REDIS_URL_HERE>
    CUBEJS_DB_TYPE: ${env.dbType}
    CUBEJS_API_SECRET: ${env.apiSecret}
    CUBEJS_APP: "\${self:service.name}-\${self:provider.stage}"
    NODE_ENV: production
    AWS_ACCOUNT_ID:
      Fn::Join:
        - ""
        - - Ref: "AWS::AccountId"

functions:
  cubejs:
    handler: index.api
    timeout: 30
    events:
      - http:
          path: /
          method: GET
      - http:
          path: /{proxy+}
          method: ANY
  cubejsProcess:
    handler: index.process
    timeout: 630
    events:
      - sns: "\${self:service.name}-\${self:provider.stage}-process"

plugins:
  - serverless-express
`;

const serverlessGoogleYml = env => `service: ${env.projectName} # NOTE: Don't put the word "google" in here

provider:
  name: google
  stage: dev
  runtime: nodejs12
  region: us-central1
  project: <YOUR_GOOGLE_PROJECT_ID_HERE>
  # The GCF credentials can be a little tricky to set up. Luckily we've documented this for you here:
  # https://serverless.com/framework/docs/providers/google/guide/credentials/
  #
  # the path to the credentials file needs to be absolute
  credentials: </path/to/service/account/keyfile.json>
  environment:
    CUBEJS_DB_TYPE: ${env.dbType}
    CUBEJS_DB_HOST: <YOUR_DB_HOST_HERE>
    CUBEJS_DB_NAME: <YOUR_DB_NAME_HERE>
    CUBEJS_DB_USER: <YOUR_DB_USER_HERE>
    CUBEJS_DB_PASS: <YOUR_DB_PASS_HERE>
    CUBEJS_DB_PORT: <YOUR_DB_PORT_HERE>
    CUBEJS_DB_BQ_PROJECT_ID: "\${self:provider.project}"
    REDIS_URL: <YOUR_REDIS_URL_HERE>
    CUBEJS_API_SECRET: ${env.apiSecret}
    CUBEJS_APP: "\${self:service.name}-\${self:provider.stage}"
    CUBEJS_SERVERLESS_PLATFORM: "\${self:provider.name}"

plugins:
  - serverless-google-cloudfunctions
  - serverless-express

# needs more granular excluding in production as only the serverless provider npm
# package should be excluded (and not the whole node_modules directory)
package:
  exclude:
    - node_modules/**
    - .gitignore
    - .git/**

functions:
  cubejs:
    handler: api
    events:
      - http: ANY
  cubejsProcess:
    handler: process
    events:
      - event:
          eventType: providers/cloud.pubsub/eventTypes/topic.publish
          resource: "projects/\${self:provider.project}/topics/\${self:service.name}-\${self:provider.stage}-process"
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

const cubeJs = `// Cube.js configuration options: https://cube.dev/docs/config
module.exports = {
};
`;

const dockerCompose = `
version: '2.2'

services:
# Example of PostgreSQL configuration, uncomment if you want to use postgres
#  postgres:
#    image: postgres:12.4
#    environment:
#      POSTGRES_USER: 'test'
#      POSTGRES_PASSWORD: 'test'
#      POSTGRES_DB: 'test'

  cube:
    image: cubejs/cube:latest
    #depends_on:
    #  - postgres
    #links:
    #  - postgres
    ports:
      # It's better to use random port binding for 4000/3000 ports
      # without it you will not able to start multiple projects inside docker
      # - 4000
      # - 3000
      # 4000 is a port for Cube.js API
      - 4000:4000
      # 3000 is a port for Playground web-server
      - 3000:3000
    env_file: .env
    volumes:
      # If you are going to use own dependencies, for example axios/vault or anything else for getting configuration
      # - .:/cube/conf
      - ./dashboard-app:/cube/conf/dashboard-app
      - ./cube.js:/cube/conf/cube.js
      - ./schema:/cube/conf/schema
`;

const templates = {
  docker: {
    scripts: {
      dev: './node_modules/.bin/cubejs-server',
    },
    files: {
      'cube.js': () => cubeJs,
      'docker-compose.yml': () => dockerCompose,
      '.env': dotEnv,
      '.gitignore': () => gitIgnore,
      'schema/Orders.js': () => ordersJs
    }
  },
  express: {
    scripts: {
      dev: 'node index.js',
    },
    files: {
      'index.js': () => indexJs,
      '.env': dotEnv,
      '.gitignore': () => gitIgnore,
      'schema/Orders.js': () => ordersJs
    }
  },
  serverless: {
    scripts: {
      dev: './node_modules/.bin/cubejs-dev-server',
    },
    files: {
      'index.js': () => handlerJs,
      'serverless.yml': serverlessYml,
      '.env': dotEnv,
      '.gitignore': () => gitIgnore,
      'schema/Orders.js': () => ordersJs
    },
    dependencies: ['@cubejs-backend/serverless', '@cubejs-backend/serverless-aws']
  },
  'serverless-google': {
    scripts: {
      dev: './node_modules/.bin/cubejs-dev-server',
    },
    files: {
      'index.js': () => handlerJs,
      'serverless.yml': serverlessGoogleYml,
      '.env': dotEnv,
      '.gitignore': () => gitIgnore,
      'schema/Orders.js': () => ordersJs
    },
    dependencies: ['@cubejs-backend/serverless', '@cubejs-backend/serverless-google'],
    devDependencies: ['serverless-google-cloudfunctions']
  }
};

export default templates;
