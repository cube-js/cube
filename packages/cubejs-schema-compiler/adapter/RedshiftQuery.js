const PostgresQuery = require('./PostgresQuery');

class RedshiftQuery extends PostgresQuery {
  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT f::timestamp date_from, t::timestamp date_to FROM (${values})`;
  }

  nowTimestampSql() {
    return 'GETDATE()';
  }
}

module.exports = RedshiftQuery;