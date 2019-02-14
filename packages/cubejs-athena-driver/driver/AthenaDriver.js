const AWS = require('aws-sdk');
const { promisify } = require('util');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const SqlString = require('sqlstring');
const crypto = require('crypto');

const applyParams = (query, params) => {
  return SqlString.format(query, params);
};

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

  queryToken(queryString) {
    return crypto.createHash('md5').update(queryString).digest('hex');
  }

  sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(() => resolve(), ms);
    })
  }

  async query(query, values) {
    const queryString = applyParams(query, values);
    const { QueryExecutionId } = await this.athena.startQueryExecutionAsync({
      QueryString: queryString,
      ResultConfiguration: {
        OutputLocation: this.config.S3OutputLocation
      },
      ClientRequestToken: this.queryToken(queryString)
    });
    while (true) {
      const queryExecution = await this.athena.getQueryExecutionAsync({
        QueryExecutionId
      });
      const status = queryExecution.QueryExecution.Status.State;
      if (
        status === 'SUCCEEDED' || status === 'FAILED' || status === 'CANCELLED'
      ) {
        const allRows = [];
        let columnInfo;
        for (
          let results = await this.athena.getQueryResultsAsync({ QueryExecutionId });
          !!results;
          results = results.NextToken && (await this.athena.getQueryResultsAsync({
            QueryExecutionId, NextToken: results.NextToken
          }))
        ) {
          const [header, ...tableRows] = results.ResultSet.Rows;
          allRows.push(...(allRows.length ? results.ResultSet.Rows : tableRows));
          if (!columnInfo) {
            columnInfo = results.ResultSet.ResultSetMetadata.ColumnInfo
          }
        }

        return allRows.map(r =>
          columnInfo
            .map((c, i) => ({ [c.Name]: r.Data[i].VarCharValue }))
            .reduce((a, b) => ({...a, ...b}), {})
        );
      }
      await this.sleep(500);
    }
  }
}

module.exports = AthenaDriver;