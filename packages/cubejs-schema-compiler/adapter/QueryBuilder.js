const postgres = require('./PostgresQuery');
const mysql = require('./MysqlQuery');
const mssql = require('./MssqlQuery');
const bigquery = require('./BigqueryQuery');
const redshift = require('./RedshiftQuery');
const prestodb = require('./PrestodbQuery');
const vertica = require('./VerticaQuery');
const snowflake = require('./SnowflakeQuery');

const ADAPTERS = {
  postgres,
  redshift,
  mysql,
  mssql,
  bigquery,
  prestodb,
  qubole_prestodb: prestodb,
  athena: prestodb,
  vertica,
  snowflake
};

exports.query = (compilers, adapter, queryOptions) => {
  if (!ADAPTERS[adapter]) {
    return null;
  }

  return new (ADAPTERS[adapter])(compilers, queryOptions);
};
