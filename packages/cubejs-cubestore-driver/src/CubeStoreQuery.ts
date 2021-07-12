import moment from 'moment-timezone';
import { BaseFilter, BaseQuery, UserError } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  year: 'year'
};

class CubeStoreFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ${this.allocateParam(param)}, '%')`;
  }
}

export class CubeStoreQuery extends BaseQuery {
  public newFilter(filter) {
    return new CubeStoreFilter(this, filter);
  }

  public convertTz(field) {
    return `CONVERT_TZ(${field}, '${moment().tz(this.timezone).format('Z')}')`;
  }

  public timeStampParam() {
    return 'to_timestamp(?)';
  }

  public timeStampCast(value) {
    return `CAST(${value} as TIMESTAMP)`; // TODO
  }

  public inDbTimeZone(date) {
    return this.inIntegrationTimeZone(date).clone().utc().format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }

  public dateTimeCast(value) {
    return `to_timestamp(${value})`;
  }

  public subtractInterval(date, interval) {
    return `DATE_SUB(${date}, INTERVAL '${interval}')`;
  }

  public addInterval(date, interval) {
    return `DATE_ADD(${date}, INTERVAL '${interval}')`;
  }

  public timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public escapeColumnName(name) {
    return `\`${name}\``;
  }

  public seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select to_timestamp('${from}') date_from, to_timestamp('${to}') date_to`
    ).join(' UNION ALL ');
    return values;
  }

  public concatStringsSql(strings) {
    return `CONCAT(${strings.join(', ')})`;
  }

  public unixTimestampSql() {
    return 'UNIX_TIMESTAMP()';
  }

  public wrapSegmentForDimensionSelect(sql) {
    return `IF(${sql}, 1, 0)`;
  }

  public hllMerge(sql) {
    return `cardinality(merge(${sql}))`;
  }

  public countDistinctApprox(sql) {
    // TODO: We should throw an error, but this gets called even when only `hllMerge` result is used.
    return `approx_distinct_is_unsupported_in_cubestore(${sql}))`;
  }
}
