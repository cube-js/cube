const crypto = require('crypto');
const dataApi = require('data-api-client');
const { BaseDriver } = require('@cubejs-backend/base-driver');

const GenericTypeToMySql = {
  string: 'varchar(255) CHARACTER SET utf8mb4',
  text: 'varchar(255) CHARACTER SET utf8mb4'
};

class AuroraServerlessMySqlDriver extends BaseDriver {
  /**
   * Returns default concurrency value.
   */
  static getDefaultConcurrency() {
    return 2;
  }

  constructor(config = {}) {
    super();
    this.config = {
      secretArn: process.env.CUBEJS_DATABASE_SECRET_ARN || config.secretArn,
      resourceArn: process.env.CUBEJS_DATABASE_CLUSTER_ARN || config.resourceArm,
      database: process.env.CUBEJS_DATABASE || config.database,
      ...config
    };

    this.dataApi = dataApi({
      secretArn: this.config.secretArn,
      resourceArn: this.config.resourceArn,
      database: this.config.database,
      options: this.config.options,
    });
  }

  async testConnection() {
    return this.dataApi.query('SELECT 1');
  }

  positionBindings(sql) {
    let questionCount = -1;
    return sql.replace(/\\?\?/g, (match) => {
      if (match === '\\?') {
        return '?';
      }

      questionCount += 1;
      return `:b${questionCount}`;
    });
  }

  async query(query, values) {
    const sql = this.positionBindings(query);

    const parameters = {};

    if (values) {
      for (let i = 0; i < values.length; i++) {
        parameters[`b${i}`] = values[i];
      }
    }

    const result = await this.dataApi.query({
      sql,
      parameters
    });

    return result.records;
  }

  informationSchemaQuery() {
    return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
  }

  quoteIdentifier(identifier) {
    return `\`${identifier}\``;
  }

  fromGenericType(columnType) {
    return GenericTypeToMySql[columnType] || super.fromGenericType(columnType);
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx) {
    if (this.config.loadPreAggregationWithoutMetaLock) {
      return this.cancelCombinator(async saveCancelFn => {
        await saveCancelFn(this.query(`${loadSql} LIMIT 0`, params));
        await saveCancelFn(this.query(loadSql.replace(/^CREATE TABLE (\S+) AS/i, 'INSERT INTO $1'), params));
      });
    }
    return super.loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx);
  }

  async downloadQueryResults(query, values) {
    if (!this.config.database) {
      throw new Error('Default database should be defined to be used for temporary tables during query results downloads');
    }
    const tableName = crypto.randomBytes(10).toString('hex');

    const transaction = this.dataApi.transaction()
      .query(`CREATE TEMPORARY TABLE \`${this.config.database}\`.t_${tableName} AS ${query} LIMIT 0`, values)
      .query(`DESCRIBE \`${this.config.database}\`.t_${tableName}`)
      .query(`DROP TEMPORARY TABLE \`${this.config.database}\`.t_${tableName}`)
      .rollback((error) => { if (error) throw new Error(error); });

    const results = await transaction.commit();
    const columns = results[1].records;

    const types = columns.map(c => ({ name: c.Field, type: this.toGenericType(c.Type) }));

    return {
      rows: await this.query(query, values),
      types,
    };
  }

  toColumnValue(value, genericType) {
    if (genericType === 'timestamp' && typeof value === 'string') {
      return value && value.replace('Z', '');
    }
    if (genericType === 'boolean' && typeof value === 'string') {
      if (value.toLowerCase() === 'true') {
        return true;
      }
      if (value.toLowerCase() === 'false') {
        return false;
      }
    }
    return super.toColumnValue(value, genericType);
  }

  async uploadTableWithIndexes(table, columns, tableData, indexesSql) {
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }
    await this.createTable(table, columns);
    try {
      const batchSize = 1000; // TODO make dynamic?
      for (let j = 0; j < Math.ceil(tableData.rows.length / batchSize); j++) {
        const currentBatchSize = Math.min(tableData.rows.length - j * batchSize, batchSize);
        const indexArray = Array.from({ length: currentBatchSize }, (v, i) => i);
        const valueParamPlaceholders =
          indexArray.map(i => `(${columns.map((c, paramIndex) => this.param(paramIndex + i * columns.length)).join(', ')})`).join(', ');
        const params = indexArray.map(i => columns
          .map(c => this.toColumnValue(tableData.rows[i + j * batchSize][c.name], c.type)))
          .reduce((a, b) => a.concat(b), []);

        await this.query(
          `INSERT INTO ${table}
            (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
          VALUES ${valueParamPlaceholders}`,
          params
        );
      }

      for (let i = 0; i < indexesSql.length; i++) {
        const [query, p] = indexesSql[i].sql;
        await this.query(query, p);
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }
}

module.exports = AuroraServerlessMySqlDriver;
