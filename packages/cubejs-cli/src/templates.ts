export type TemplateFileContext = {
  dbType: string,
  apiSecret: string,
  dockerVersion: string,
  driverEnvVariables?: string[],
};

export type Template = {
  scripts: Record<string, string>,
  files: Record<string, (ctx: TemplateFileContext) => string>,
};

// Shared environment variables, across all DB types
const sharedDotEnvVars = env => `CUBEJS_DEV_MODE=true
CUBEJS_DB_TYPE=${env.dbType}
CUBEJS_API_SECRET=${env.apiSecret}
CUBEJS_EXTERNAL_DEFAULT=true
CUBEJS_SCHEDULED_REFRESH_DEFAULT=true
CUBEJS_SCHEMA_PATH=model`;

const defaultDotEnvVars = env => `# Cube environment variables: https://cube.dev/docs/reference/environment-variables
${sharedDotEnvVars(env)}
CUBEJS_WEB_SOCKETS=true`;

const athenaDotEnvVars = env => `# Cube environment variables: https://cube.dev/docs/reference/environment-variables
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

const ordersYml = `cubes:
  - name: orders
    sql: >
      SELECT 1 AS id, 100 AS amount, 'new' status
      UNION ALL
      SELECT 2 AS id, 200 AS amount, 'new' status
      UNION ALL
      SELECT 3 AS id, 300 AS amount, 'processed' status
      UNION ALL
      SELECT 4 AS id, 500 AS amount, 'processed' status
      UNION ALL
      SELECT 5 AS id, 600 AS amount, 'shipped' status

    # Pre-aggregation definitions go here.
    # Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
    # pre_aggregations:

    measures:
      - name: count
        type: count

      - name: total_amount
        sql: amount
        type: sum

    dimensions:
      - name: status
        sql: status
        type: string
`;

const exampleViewYml = `# In Cube, views are used to expose slices of your data graph and act as data marts.
# You can control which measures and dimensions are exposed to BIs or data apps,
# as well as the direction of joins between the exposed cubes.
# You can learn more about views in documentation here - https://cube.dev/docs/schema/reference/view

# The following example shows a view defined on top of orders and customers cubes.
# Both orders and customers cubes are exposed using the "includes" parameter to
# control which measures and dimensions are exposed.
# Prefixes can also be applied when exposing measures or dimensions.
# In this case, the customers' city dimension is prefixed with the cube name,
# resulting in "customers_city" when querying the view.

# views:
#   - name: example_view
#
#     cubes:
#       - join_path: orders
#         includes:
#           - status
#           - created_date
#
#           - total_amount
#           - count
#
#       - join_path: orders.customers
#         prefix: true
#         includes:
#           - city
`;

const cubeJs = `// Cube configuration options: https://cube.dev/docs/config
/** @type{ import('@cubejs-backend/server-core').CreateOptions } */
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
      - 4000:4000  # Cube API and Developer Playground
      - 3000:3000  # Dashboard app, if created
    env_file: .env
    volumes:
      - .:/cube/conf
      # We ignore Cube deps, because they are built-in inside the official Docker image
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
      'model/cubes/orders.yml': () => ordersYml,
      'model/views/example_view.yml': () => exampleViewYml,
    }
  }
};

export default templates;
