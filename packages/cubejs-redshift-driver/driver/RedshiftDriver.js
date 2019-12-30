const Redshift = require('aws-sdk/clients/redshift');
const pg = require('pg');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

const { Pool } = pg;

class RedshiftDriver extends PostgresDriver {
  constructor(config) {
    super(config);
    this.config = config || {};
  }

  async getCredentialsFromAWS() {
    const [clusterIdentifier] = process.env.CUBEJS_DB_HOST && process.env.CUBEJS_DB_HOST.split('.');
    let credentials;

    try {
      credentials = await (new Redshift()).getClusterCredentials({
        ClusterIdentifier: clusterIdentifier,
        DbUser: process.env.CUBEJS_DB_USER,
      }).promise();
    } catch (error) {
      console.log(`An error occurred while trying to retrieve Redshift credentials: ${error.stack || error}`);
    }

    if (!credentials || !credentials.DbUser && !credentials.DbPassword) {
      throw new Error('Unable to retrieve Redshift credentials from AWS');
    }

    return {
      user: credentials.DbUser,
      password: credentials.DbPassword
    };
  }

  getCredentialsFromEnv() {
    return {
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
    };
  }

  async createPool() {
    if (this.pool && this.pool instanceof Pool) {
      return;
    }

    const credentials = process.env.CUBEJS_DB_PASS ?
      this.getCredentialsFromEnv() :
      await this.getCredentialsFromAWS();

    this.pool = new Pool({
      max: 8,
      idleTimeoutMillis: 30000,
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT,
      ssl: (process.env.CUBEJS_DB_SSL || 'false').toLowerCase() === 'true' ? {} : undefined,
      ...credentials,
      ...this.config
    });
    this.pool.on('error', (err) => {
      console.log(`Unexpected error on idle client: ${err.stack || err}`); // TODO
    });
  }

  async testConnection() {
    await this.createPool();
    return super.testConnection();
  }

  async query(query, values) {
    await this.createPool();
    return super.query(query, values);
  }

  release() {
    return this.pool && this.pool.end();
  }
}

module.exports = RedshiftDriver;
