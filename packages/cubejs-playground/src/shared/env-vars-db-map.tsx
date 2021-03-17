import logoPostgres from '../img/db/postgres.svg';
import logoMysql from '../img/db/mysql.svg';
import logoAthena from '../img/db/athena.svg';
import logoClickhouse from '../img/db/clickhouse.svg';
import logoHive from '../img/db/hive.svg';
import logoRedshift from '../img/db/redshift.svg';
import logoPresto from '../img/db/presto.svg';
import logoSnowflake from '../img/db/snowflake.svg';
import logoOracle from '../img/db/oracle.svg';
import logoMssql from '../img/db/mssql.svg';
import logoBigquery from '../img/db/bigquery.svg';
import logoMongodb from '../img/db/mongodb.svg';
import logoDruid from '../img/db/druid.svg';

const BASE_SERVER = [
  { env: 'CUBEJS_DB_HOST', title: 'Hostname' },
  { env: 'CUBEJS_DB_PORT', title: 'Port' },
];

const BASE_CRED = [
  { env: 'CUBEJS_DB_USER', title: 'Username' },
  { env: 'CUBEJS_DB_PASS', title: 'Password' },
];

const DB_NAME = { env: 'CUBEJS_DB_NAME', title: 'Database' };

const envVarsDbMap = [
  {
    databases: [
      { title: 'PostgreSQL', driver: 'postgres', logo: logoPostgres },
      { title: 'MySQL', driver: 'mysql', logo: logoMysql },
      { title: 'AWS Redshift', drive: 'redshift', logo: logoRedshift },
      { title: 'ClickHouse', driver: 'clickhouse', logo: logoClickhouse },
      { title: 'Hive/SparkSQL', driver: 'hive', logo: logoHive },
      { title: 'Oracle', driver: 'oracle', logo: logoOracle },
    ],
    settings: [...BASE_SERVER, DB_NAME, ...BASE_CRED],
  },
  {
    databases: [{ title: 'MS SQL', driver: 'mssql', logo: logoMssql }],
    settings: [
      ...BASE_SERVER,
      ...BASE_CRED,
      DB_NAME,
      { env: 'CUBEJS_DB_DOMAIN', title: 'Domain Name' },
    ],
  },
  {
    databases: [
      {
        title: 'AWS Athena',
        driver: 'athena',
        logo: logoAthena,
        instructions: `
Specify the AWS access and secret keys with the <a href="https://docs.aws.amazon.com/athena/latest/ug/security-iam-athena.html" target="_blank">access necessary to run Athena queries</a>, 
and the target AWS region and <a href="https://docs.aws.amazon.com/athena/latest/ug/querying.html" target="_blank">S3 output location</a> where query results are stored.
    `,
      },
    ],
    settings: [
      { env: 'CUBEJS_AWS_KEY', title: 'AWS Access Key ID' },
      { env: 'CUBEJS_AWS_SECRET', title: 'AWS Secret Access Key' },
      { env: 'CUBEJS_AWS_REGION', title: 'AWS Region' },
      { env: 'CUBEJS_AWS_S3_OUTPUT_LOCATION', title: 'S3 Output Location' },
    ],
  },
  {
    databases: [
      {
        title: 'Google Bigquery',
        driver: 'bigquery',
        logo: logoBigquery,
        instructions: `
Upload a service account JSON keyfile to connect to BigQuery.<br/>Alternatively, you can encode the key file with base64 and specify it manually.
    `,
      },
    ],
    settings: [
      {
        env: 'CUBEJS_DB_BQ_KEY_FILE',
        title: 'Upload a Service Account JSON file',
        type: 'base64upload',
        uploadTarget: 'CUBEJS_DB_BQ_CREDENTIALS',
        extractField: {
          jsonField: 'project_id',
          formField: 'CUBEJS_DB_BQ_PROJECT_ID',
        },
      },
      { env: 'CUBEJS_DB_BQ_PROJECT_ID', title: 'Project ID' },
      { env: 'CUBEJS_DB_BQ_CREDENTIALS', title: 'Encoded Key File' },
    ],
  },
  {
    databases: [{ title: 'MongoDB', driver: 'mongobi', logo: logoMongodb }],
    settings: [
      ...BASE_SERVER,
      ...BASE_CRED,
      DB_NAME,
      // { env: 'CUBEJS_DB_SSL', title: '' },
      // { env: 'CUBEJS_DB_SSL_CA', title: '' },
      // { env: 'CUBEJS_DB_SSL_CERT', title: '' },
      // { env: 'CUBEJS_DB_SSL_CIPHERS' },
      // { env: 'CUBEJS_DB_SSL_PASSPHRASE' }
    ],
  },
  {
    databases: [
      { title: 'Snowflake', driver: 'snowflake', logo: logoSnowflake },
    ],
    settings: [
      ...BASE_CRED,
      DB_NAME,
      { env: 'CUBEJS_DB_SNOWFLAKE_ACCOUNT', title: 'Account' },
      { env: 'CUBEJS_DB_SNOWFLAKE_REGION', title: 'Region' },
      { env: 'CUBEJS_DB_SNOWFLAKE_WAREHOUSE', title: 'Warehouse' },
      { env: 'CUBEJS_DB_SNOWFLAKE_ROLE', title: 'Role' },
    ],
  },
  {
    databases: [{ title: 'Presto', driver: 'prestodb', logo: logoPresto }],
    settings: [
      ...BASE_SERVER,
      ...BASE_CRED,
      { env: 'CUBEJS_DB_CATALOG', title: 'Catalog' },
      { env: 'CUBEJS_DB_SCHEMA', title: 'Schema' },
    ],
  },
  {
    databases: [{ title: 'Druid', driver: 'druid', logo: logoDruid }],
    settings: [{ env: 'CUBEJS_DB_URL', title: 'URL' }, ...BASE_CRED],
  },
];

export default envVarsDbMap;
