import { parseSqlInterval } from '@cubejs-backend/shared';
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

  /**
   * Redshift doesn't support Interval values with month or year parts (as Postgres does)
   * so we need to make date math on our own.
   */
  public override subtractInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let result = date;

    for (const [datePart, intervalValue] of Object.entries(intervalParsed)) {
      result = `DATEADD(${datePart}, -${intervalValue}, ${result})`;
    }

    return result;
  }

  /**
   * Redshift doesn't support Interval values with month or year parts (as Postgres does)
   * so we need to make date math on our own.
   */
  public override addInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let result = date;

    for (const [datePart, intervalValue] of Object.entries(intervalParsed)) {
      result = `DATEADD(${datePart}, ${intervalValue}, ${result})`;
    }

    return result;
  }

  /**
   * Redshift doesn't support Interval values with month or year parts (as Postgres does)
   * so we need to make date math on our own.
   */
  public override dateBin(interval: string, source: string, origin: string): string {
    const intervalParsed = parseSqlInterval(interval);

    if ((intervalParsed.year || intervalParsed.month || intervalParsed.quarter) &&
        (intervalParsed.week || intervalParsed.day || intervalParsed.hour || intervalParsed.minute || intervalParsed.second)) {
      throw new Error(`Complex intervals like "${interval}" are not supported. Please use Year to Month or Day to second intervals`);
    }

    if (intervalParsed.year || intervalParsed.month || intervalParsed.quarter) {
      let totalMonths = 0;

      if (intervalParsed.year) {
        totalMonths += intervalParsed.year * 12;
      }

      if (intervalParsed.quarter) {
        totalMonths += intervalParsed.quarter * 3;
      }

      if (intervalParsed.month) {
        totalMonths += intervalParsed.month;
      }

      return `DATEADD(
      month,
      (FLOOR(DATEDIFF(month, ${this.dateTimeCast(`'${origin}'`)}, ${source}) / ${totalMonths}) * ${totalMonths})::int,
      ${this.dateTimeCast(`'${origin}'`)}
    )`;
    }

    // For days and lower intervals - we can reuse Postgres version
    return super.dateBin(interval, source, origin);
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DLOG10 = 'LOG(10, {{ args_concat }})';
    templates.functions.DATEDIFF = 'DATEDIFF({{ date_part }}, {{ args[1] }}, {{ args[2] }})';
    delete templates.functions.COVAR_POP;
    delete templates.functions.COVAR_SAMP;
    delete templates.window_frame_types.range;
    delete templates.window_frame_types.groups;
    templates.types.binary = 'VARBINARY';
    return templates;
  }
}
