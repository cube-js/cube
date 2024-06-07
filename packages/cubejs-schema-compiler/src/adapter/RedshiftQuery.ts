import { PostgresQuery } from './PostgresQuery';

export class RedshiftQuery extends PostgresQuery {
  public seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT dates.f::timestamp date_from, dates.t::timestamp date_to FROM (${values}) dates`;
  }

  public nowTimestampSql() {
    return 'GETDATE()';
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DLOG10 = 'LOG(10, {{ args_concat }})';
    templates.functions.DATEDIFF = 'DATEDIFF({{ date_part }}, {{ args[1] }}, {{ args[2] }})';
    delete templates.functions.COVAR_POP;
    delete templates.functions.COVAR_SAMP;
    delete templates.window_frame_types.range;
    delete templates.window_frame_types.groups;
    return templates;
  }
}
