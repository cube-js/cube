/* eslint-disable no-underscore-dangle */
const R = require('ramda');
const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
const { getEnv } = require('@cubejs-backend/shared');

function pause(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const suffixTableRegex = /^(.*?)([0-9_]+)$/;

class BigQueryDriver extends BaseDriver {
  constructor(config = {}) {
    super();

    this.options = {
      scopes: ['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/drive'],
      projectId: process.env.CUBEJS_DB_BQ_PROJECT_ID,
      keyFilename: process.env.CUBEJS_DB_BQ_KEY_FILE,
      credentials: process.env.CUBEJS_DB_BQ_CREDENTIALS ?
        JSON.parse(Buffer.from(process.env.CUBEJS_DB_BQ_CREDENTIALS, 'base64').toString('utf8')) :
        undefined,
      exportBucket: process.env.CUBEJS_DB_BQ_EXPORT_BUCKET,
      ...config,
      pollTimeout: (config.pollTimeout || getEnv('dbPollTimeout')) * 1000,
      pollMaxInterval: (config.pollMaxInterval || getEnv('dbPollMaxInterval')) * 1000,
    };

    this.bigquery = new BigQuery(this.options);
    if (this.options.exportBucket) {
      this.storage = new Storage(this.options);
      this.bucket = this.storage.bucket(this.options.exportBucket);
    }

    this.mapFieldsRecursive = this.mapFieldsRecursive.bind(this);
    this.tablesSchema = this.tablesSchema.bind(this);
    this.parseDataset = this.parseDataset.bind(this);
    this.parseTableData = this.parseTableData.bind(this);
    this.flatten = this.flatten.bind(this);
    this.toObjectFromId = this.toObjectFromId.bind(this);
  }

  static driverEnvVariables() {
    return ['CUBEJS_DB_BQ_PROJECT_ID', 'CUBEJS_DB_BQ_KEY_FILE'];
  }

  testConnection() {
    return this.bigquery.query({
      query: 'SELECT ? AS number', params: ['1']
    });
  }

  readOnly() {
    return !!this.options.readOnly;
  }

  async query(query, values, options) {
    const data = await this.runQueryJob({
      query,
      params: values,
      parameterMode: 'positional',
      useLegacySql: false
    }, options);

    return data[0] && data[0].map(
      row => R.map(value => (value && value.value && typeof value.value === 'string' ? value.value : value), row)
    );
  }

  toObjectFromId(accumulator, currentElement) {
    accumulator[currentElement.id] = currentElement.data;
    return accumulator;
  }

  reduceSuffixTables(accumulator, currentElement) {
    const suffixMatch = currentElement.id.toString().match(suffixTableRegex);
    if (suffixMatch) {
      accumulator.__suffixMatched = accumulator.__suffixMatched || {};
      accumulator.__suffixMatched[suffixMatch[1]] = accumulator.__suffixMatched[suffixMatch[1]] || [];
      accumulator.__suffixMatched[suffixMatch[1]].push(currentElement);
    } else {
      accumulator[currentElement.id] = currentElement.data;
    }
    return accumulator;
  }

  addSuffixTables(accumulator) {
    // eslint-disable-next-line no-restricted-syntax,guard-for-in
    for (const prefix in accumulator.__suffixMatched) {
      const suffixMatched = accumulator.__suffixMatched[prefix];
      const sorted = suffixMatched.sort((a, b) => b.toString().localeCompare(a.toString()));
      for (let i = 0; i < Math.min(10, sorted.length); i++) {
        accumulator[sorted[i].id] = sorted[i].data;
      }
    }
    delete accumulator.__suffixMatched;
    return accumulator;
  }

  flatten(list) {
    return list.reduce(
      (a, b) => a.concat(Array.isArray(b) ? this.flatten(b) : b), []
    );
  }

  mapFieldsRecursive(field) {
    if (field.type === "RECORD") {
      return this.flatten(field.fields.map(this.mapFieldsRecursive)).map(
        (nestedField) => ({ name: `${field.name}.${nestedField.name}`, type: nestedField.type })
      );
    }
    return field;
  }

  parseDataset(dataset) {
    return dataset.getTables().then(
      (data) => Promise.all(data[0].map(this.parseTableData))
        .then(tables => ({ id: dataset.id, data: this.addSuffixTables(tables.reduce(this.reduceSuffixTables, {})) }))
    );
  }

  parseTableData(table) {
    return table.getMetadata().then(
      (data) => ({
        id: table.id,
        data: this.flatten(((data[0].schema && data[0].schema.fields) || []).map(this.mapFieldsRecursive))
      })
    );
  }

  tablesSchema() {
    return this.bigquery.getDatasets().then((data) => Promise.all(data[0].map(this.parseDataset))
      .then(innerData => innerData.reduce(this.toObjectFromId, {})));
  }

  async getTablesQuery(schemaName) {
    try {
      const dataSet = await this.bigquery.dataset(schemaName);
      if (!dataSet) {
        return [];
      }
      const [tables] = await this.bigquery.dataset(schemaName).getTables();
      return tables.map(t => ({ table_name: t.id }));
    } catch (e) {
      if (e.toString().indexOf('Not found')) {
        return [];
      }
      throw e;
    }
  }

  async tableColumnTypes(table) {
    const [schema, name] = table.split('.');
    const [bigQueryTable] = await this.bigquery.dataset(schema).table(name).getMetadata();
    return bigQueryTable.schema.fields.map(c => ({ name: c.name, type: this.toGenericType(c.type) }));
  }

  async createSchemaIfNotExists(schemaName) {
    await this.bigquery.dataset(schemaName).get({ autoCreate: true });
  }

  async downloadTable(table, options) {
    if (options && options.csvImport && this.bucket) {
      const destination = this.bucket.file(`${table}-*.csv.gz`);
      const [schema, tableName] = table.split('.');
      const bigQueryTable = this.bigquery.dataset(schema).table(tableName);
      const [job] = await bigQueryTable.createExtractJob(destination, { format: 'CSV', gzip: true });
      await this.waitForJobResult(job, { table }, false);
      const [files] = await this.bucket.getFiles({ prefix: `${table}-` });
      const urls = await Promise.all(files.map(async file => {
        const [url] = await file.getSignedUrl({
          action: 'read',
          expires: new Date(new Date().getTime() + 60 * 60 * 1000)
        });
        return url;
      }));
      return { csvFile: urls };
    }
    return super.downloadTable(table, options);
  }

  async loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, options) {
    const [dataSet, tableName] = preAggregationTableName.split('.');
    const bigQueryQuery = {
      query: loadSql,
      params,
      parameterMode: 'positional',
      destination: this.bigquery.dataset(dataSet).table(tableName),
      createDisposition: "CREATE_IF_NEEDED",
      useLegacySql: false
    };
    return this.runQueryJob(bigQueryQuery, options, false);
  }

  async awaitForJobStatus(job, options, withResults) {
    const [result] = await job.getMetadata();
    if (result.status && result.status.state === 'DONE') {
      if (result.status.errorResult) {
        throw new Error(
          result.status.errorResult.message ?
            result.status.errorResult.message :
            JSON.stringify(result.status.errorResult)
        );
      }
      this.reportQueryUsage(result.statistics, options);
    } else {
      return null;
    }

    return withResults ? job.getQueryResults() : true;
  }

  async runQueryJob(bigQueryQuery, options, withResults = true) {
    const [job] = await this.bigquery.createQueryJob(bigQueryQuery);
    return this.waitForJobResult(job, options, withResults);
  }

  async waitForJobResult(job, options, withResults) {
    const startedTime = Date.now();

    for (let i = 0; Date.now() - startedTime <= this.options.pollTimeout; i++) {
      const result = await this.awaitForJobStatus(job, options, withResults);
      if (result) {
        return result;
      }

      await pause(
        Math.min(this.options.pollMaxInterval, 200 * i),
      );
    }

    throw new Error(
      `BigQuery job timeout reached ${this.options.pollTimeout}ms`,
    );
  }

  quoteIdentifier(identifier) {
    const nestedFields = identifier.split('.');
    return nestedFields.map(name => {
      if (name.match(/^[a-z0-9_]+$/)) {
        return name;
      }
      return `\`${identifier}\``;
    }).join('.');
  }
}

module.exports = BigQueryDriver;
