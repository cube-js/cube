import { PostgresQuery } from './PostgresQuery';

export class RedshiftQuery extends PostgresQuery {
  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT dates.f::timestamp date_from, dates.t::timestamp date_to FROM (${values}) dates`;
  }

  nowTimestampSql() {
    return 'GETDATE()';
  }

  sqlTemplates() {
    return {
      ...super.sqlTemplates(),
      COVAR_POP: undefined,
      COVAR_SAMP: undefined,
      DLOG10: 'LOG(10, {{ args_concat }})',
    };
  }
}
