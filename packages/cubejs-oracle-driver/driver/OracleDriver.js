/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `OracleDriver` and related types declaration.
 */

const {
  getEnv,
  assertDataSource,
} = require('@cubejs-backend/shared');
const { BaseDriver, TableColumn } = require('@cubejs-backend/base-driver');
const oracledb = require('oracledb');
const { reduce } = require('ramda');

// Maps Oracle `metaData.dbTypeName` strings to Cube generic types. NUMBER and the
// TIMESTAMP* family are handled separately (scale-based / prefix match) below.
const OracleTypeToGenericType = {
  varchar2: 'text',
  nvarchar2: 'text',
  char: 'text',
  nchar: 'text',
  clob: 'text',
  nclob: 'text',
  long: 'text',
  binary_float: 'float',
  binary_double: 'double',
  date: 'timestamp',
  'number': 'decimal',
};

const sortByKeys = (unordered) => {
  const ordered = {};

  Object.keys(unordered).sort().forEach((key) => {
    ordered[key] = unordered[key];
  });

  return ordered;
};

const reduceCb = (result, i) => {
  let schema = (result[i.table_schema] || {});
  let tables = (schema[i.table_name] || []);
  let attributes = new Array();

  if (i.key_type === "P" || i.key_type === "U") {
    attributes.push(["primaryKey"]);
  }

  tables.push({
    name: i.column_name,
    type: i.data_type,
    attributes
  });

  schema[i.table_name] = tables.sort();
  result[i.table_schema] = sortByKeys(schema);

  return sortByKeys(result);
};

class OracleDriver extends BaseDriver {
  static getDefaultConcurrency() {
    return 2;
  }

  constructor(config = {}) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');
    const preAggregations = config.preAggregations || false;

    this.db = oracledb;
    this.db.outFormat = this.db.OBJECT;
    this.db.partRows = 100000;
    this.db.maxRows = 100000;
    this.db.prefetchRows = 500;
    this.config = {
      user: getEnv('dbUser', { dataSource, preAggregations }),
      password: getEnv('dbPass', { dataSource, preAggregations }),
      db: getEnv('dbName', { dataSource, preAggregations }),
      host: getEnv('dbHost', { dataSource, preAggregations }),
      port: getEnv('dbPort', { dataSource, preAggregations }) || 1521,
      poolMin: 0,
      poolMax:
        config.maxPoolSize ||
        getEnv('dbMaxPoolSize', { dataSource, preAggregations }) ||
        50,
      ...config
    };
    this.config.connectionString = this.config.connectionString || `${this.config.host}:${this.config.port}/${this.config.db}`;
  }

  async tablesSchema() {
    const data = await this.query(`
      select tc.owner         "table_schema"
          , tc.table_name     "table_name"
          , tc.column_name    "column_name"
          , tc.data_type      "data_type"
          , c.constraint_type "key_type"
      from all_tab_columns tc
      left join all_cons_columns cc
        on (tc.owner, tc.table_name, tc.column_name)
        in ((cc.owner, cc.table_name, cc.column_name))
      left join all_constraints c
        on (tc.owner, tc.table_name, cc.constraint_name)
        in ((c.owner, c.table_name, c.constraint_name))
        and c.constraint_type
        in ('P','U')
      where tc.owner = user
    `);

    return reduce(reduceCb, {}, data);
  }

  /**
   * Runs once per pooled session. Aligns the session NLS formats with the ISO-ish
   * date strings Cube binds, so implicit string→DATE/TIMESTAMP conversions (e.g.
   * the native planner's `CAST(? AS TIMESTAMP)` over a 'YYYY-MM-DD' filter bound)
   * parse instead of failing with ORA-01843 under Oracle's default NLS. Explicit
   * TO_DATE/TO_TIMESTAMP calls carry their own masks and are unaffected.
   * @protected
   */
  static initConnection(connection, requestedTag, cb) {
    connection.execute(
      "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD' NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'",
      (err) => cb(err)
    );
  }

  async getConnectionFromPool() {
    if (!this.pool) {
      this.pool = await this.db.createPool({ ...this.config, sessionCallback: OracleDriver.initConnection });
    }

    return this.pool.getConnection()
  }

  async testConnection() {
    await this.query('SELECT 1 FROM DUAL', {});
  }

  async createTable(quotedTableName, columns) {
    if (quotedTableName.length > 128) {
      throw new Error('Oracle can not work with table names longer than 128 symbols. ' +
        `Consider using the 'sqlAlias' attribute in your cube definition for ${quotedTableName}.`);
    }

    return super.createTable(quotedTableName, columns);
  }

  static normalizeParams(query, values) {
    if (!values || values.length === 0) {
      return { sql: query, binds: {} };
    }

    const binds = {};
    const valueToName = new Map();
    let idx = 0;
    let nextName = 0;

    // `:"?"` must be matched as a whole before a lone `?`, so it appears first
    // in the alternation; since it starts with `:`, its inner `?` is consumed
    // as part of the match and never matched again on its own.
    //
    // Placeholders carrying the same value share a single named bind. This is
    // semantically identical (the same value is bound) and keeps repeated
    // expressions textually identical across clauses — required by Oracle, which
    // otherwise rejects e.g. a CASE expression in both SELECT and GROUP BY when
    // its param renders as two different bind names (ORA-00979).
    const sql = query.replace(/:"\?"|\?/g, () => {
      const value = values[idx];
      idx += 1;
      // A Map distinguishes values by SameValueZero, so 1 and '1' stay separate;
      // the raw value works as the key without stringifying.
      let name = valueToName.get(value);
      if (name === undefined) {
        name = `cb_param_${nextName}`;
        nextName += 1;
        valueToName.set(value, name);
        binds[name] = value;
      }
      return `:${name}`;
    });

    return { sql, binds };
  }

  async query(query, values) {
    const conn = await this.getConnectionFromPool();

    try {
      const { sql, binds } = OracleDriver.normalizeParams(query, values);
      const res = await conn.execute(sql, binds);
      return res && res.rows;
    } catch (e) {
      throw (e);
    } finally {
      try {
        await conn.close();
      } catch (e) {
        throw e;
      }
    }
  }

  static metaDataToColumnTypes(metaData) {
    return (metaData || []).map((column) => {
      const dbTypeName = (column.dbTypeName || '').toLowerCase();
      let type = 'text';

      if (dbTypeName.startsWith('timestamp')) {
        type = 'timestamp';
      } else {
        type = OracleTypeToGenericType[dbTypeName] || 'text';
      }

      return { name: column.name, type };
    });
  }

  async downloadQueryResults(query, values, _options) {
    const conn = await this.getConnectionFromPool();

    try {
      const { sql, binds } = OracleDriver.normalizeParams(query, values);
      const res = await conn.execute(sql, binds);
      return {
        rows: (res && res.rows) || [],
        types: OracleDriver.metaDataToColumnTypes(res && res.metaData),
      };
    } finally {
      try {
        await conn.close();
      } catch (e) {
        throw e;
      }
    }
  }

  release() {
    return this.pool && this.pool.close();
  }

  readOnly() {
    return true;
  }

  wrapQueryWithLimit(query) {
    // Oracle forbids the `AS` keyword for table/subquery aliases.
    query.query = `SELECT * FROM (${query.query}) t WHERE ROWNUM <= ${query.limit}`;
  }
}

module.exports = OracleDriver;
