const AWS = require('aws-sdk');
const { promisify } = require('util');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const SqlString = require('sqlstring');

const applyParams = (query, params) => SqlString.format(query, params);

class AthenaDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      accessKeyId: process.env.CUBEJS_AWS_KEY,
      secretAccessKey: process.env.CUBEJS_AWS_SECRET,
      region: process.env.CUBEJS_AWS_REGION,
      S3OutputLocation: process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION,
      ...config
    };
    this.athena = new AWS.Athena(this.config);
    this.athena.startQueryExecutionAsync = promisify(this.athena.startQueryExecution.bind(this.athena));
    this.athena.stopQueryExecutionAsync = promisify(this.athena.stopQueryExecution.bind(this.athena));
    this.athena.getQueryResultsAsync = promisify(this.athena.getQueryResults.bind(this.athena));
    this.athena.getQueryExecutionAsync = promisify(this.athena.getQueryExecution.bind(this.athena));
  }

  async testConnection() {
    return this.query('SELECT 1', []);
  }

  sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(() => resolve(), ms);
    });
  }

  async query(query, values, options) {
    const queryString = applyParams(
      query,
      (values || []).map(s => (typeof s === 'string' ? {
        toSqlString: () => SqlString.escape(s).replace(/\\\\([_%])/g, '\\$1').replace(/\\'/g, '\'\'')
      } : s))
    );
    const { QueryExecutionId } = await this.athena.startQueryExecutionAsync({
      QueryString: queryString,
      ResultConfiguration: {
        OutputLocation: this.config.S3OutputLocation
      }
    });
    while (true) {
      const queryExecution = await this.athena.getQueryExecutionAsync({
        QueryExecutionId
      });
      const status = queryExecution.QueryExecution.Status.State;
      if (status === 'FAILED') {
        throw new Error(queryExecution.QueryExecution.Status.StateChangeReason);
      }
      if (status === 'CANCELLED') {
        throw new Error('Query has been cancelled');
      }
      if (
        status === 'SUCCEEDED'
      ) {
        const allRows = [];
        let columnInfo;
        this.reportQueryUsage({
          dataScannedInBytes: queryExecution.QueryExecution.Statistics.DataScannedInBytes
        }, options);
        for (
          let results = await this.athena.getQueryResultsAsync({ QueryExecutionId });
          results;
          results = results.NextToken && (await this.athena.getQueryResultsAsync({
            QueryExecutionId, NextToken: results.NextToken
          }))
        ) {
          const [header, ...tableRows] = results.ResultSet.Rows;
          allRows.push(...(allRows.length ? results.ResultSet.Rows : tableRows));
          if (!columnInfo) {
            columnInfo = /SHOW COLUMNS/.test(query) // Fix for getColumns method
              ? [{ Name: 'column' }]
              : results.ResultSet.ResultSetMetadata.ColumnInfo;
          }
        }

        return allRows.map(r => columnInfo
          .map((c, i) => ({ [c.Name]: r.Data[i].VarCharValue }))
          .reduce((a, b) => ({ ...a, ...b }), {}));
      }
      await this.sleep(500);
    }
  }

  async tablesSchema() {
    const tablesSchema = await super.tablesSchema();
    const viewsSchema = await this.viewsSchema(tablesSchema);
    
    return this.mergeSchemas([tablesSchema, viewsSchema]);
  }

  async viewsSchema(tablesSchema) {
    // eslint-disable-next-line camelcase
    const isView = ({ table_schema, table_name }) => !tablesSchema[table_schema]
      || !tablesSchema[table_schema][table_name];

    const allTables = await this.getAllTables();
    const arrViewsSchema = await Promise.all(
      allTables
        .filter(isView)
        .map(table => this.getColumns(table))
    );

    return this.mergeSchemas(arrViewsSchema);
  }

  async getAllTables() {
    const data = await this.query(`
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE tables.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    `);

    return data;
  }

  // eslint-disable-next-line camelcase
  async getColumns({ table_schema, table_name } = {}) {
    // eslint-disable-next-line camelcase
    const data = await this.query(`SHOW COLUMNS IN "${table_schema}"."${table_name}"`);

    return {
      [table_schema]: {
        [table_name]: data.map(({ column }) => {
          const [name, type] = column.split('\t');
          return { name, type, attributes: [] };
        })
      }
    };
  }

  mergeSchemas(arrSchemas) {
    const result = {};

    arrSchemas.forEach(schemas => {
      Object.keys(schemas).forEach(schema => {
        Object.keys(schemas[schema]).forEach((name) => {
          if (!result[schema]) result[schema] = {};
          if (!result[schema][name]) result[schema][name] = schemas[schema][name];
        });
      });
    });

    return result;
  }
}

module.exports = AthenaDriver;
