export type TemplateFileContext = {
  dbType: string,
  apiSecret: string,
  projectName: string,
  dockerVersion: string,
  driverEnvVariables?: string[],
};

export type Template = {
  scripts: Record<string, string>,
  files: Record<string, (ctx: TemplateFileContext) => string>,
  dependencies?: string[],
  devDependencies?: string[],
};

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
CUBEJS_API_SECRET=${env.apiSecret}
CUBEJS_EXTERNAL_DEFAULT=true
CUBEJS_SCHEDULED_REFRESH_DEFAULT=true`;

const defaultDotEnvVars = env => `# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables
${sharedDotEnvVars(env)}
CUBEJS_WEB_SOCKETS=true`;

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
.cubestore
upstream
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
    CUBEJS_REDIS_URL: <YOUR_REDIS_URL_HERE>
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
          cors:
            origin: '*'
            headers:
              - Content-Type
              - Authorization
              - X-Request-Id
              - X-Amz-Date
              - X-Amz-Security-Token
              - X-Api-Key
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
    CUBEJS_REDIS_URL: <YOUR_REDIS_URL_HERE>
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

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },

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

const dockerCompose = (ctx: TemplateFileContext) => `
version: '2.2'

services:
  cube:
    image: cubejs/cube:${ctx.dockerVersion}
    ports:
      # It's better to use random port binding for 4000/3000 ports
      # without it you will not able to start multiple projects inside docker
      - 4000:4000  # Cube.js API and Developer Playground
      - 3000:3000  # Dashboard app, if created
    env_file: .env
    volumes:
      - .:/cube/conf
      # We ignore Cube.js deps, because they are built-in inside the official Docker image
      - .empty:/cube/conf/node_modules/@cubejs-backend/
`;

const templates: Record<string, Template> = {
  docker: {
    scripts: {
      dev: 'cubejs-server',
    },
    files: {
      'cube.js': () => cubeJs,
      'docker-compose.yml': dockerCompose,
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
      dev: 'cubejs-dev-server',
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
      dev: 'cubejs-dev-server',
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
