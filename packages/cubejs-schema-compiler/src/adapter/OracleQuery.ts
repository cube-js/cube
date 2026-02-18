import { parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import type { BaseDimension } from './BaseDimension';

const GRANULARITY_VALUE = {
  day: 'DD',
  week: 'IW',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  year: 'YYYY'
};

class OracleFilter extends BaseFilter {
  public castParameter() {
    return ':"?"';
  }

  /**
   * "ILIKE" is not supported
   */
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} LIKE ${p}${this.allocateParam(param)}${s}`;
  }
}

export class OracleQuery extends BaseQuery {
  private static readonly ORACLE_TZ_FORMAT_WITH_Z = 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"';

  private static readonly ORACLE_TZ_FORMAT_NO_Z = 'YYYY-MM-DD"T"HH24:MI:SS.FF';

  /**
   * Determines if a value represents a SQL identifier (column name) rather than a bind parameter.
   * Handles both unquoted identifiers (e.g., "date_from", "table.column") and quoted
   * identifiers (e.g., "date_from", "table"."column").
   */
  private isIdentifierToken(value: string): boolean {
    return (
      /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(value) ||
      /^"[^"]+"(\."[^"]+")*$/.test(value)
    );
  }

  /**
   * Generates Oracle TO_TIMESTAMP_TZ function call for timezone-aware timestamp conversion.
   *
   * The format string must match the actual data format:
   * - Filter parameters ('?') come as ISO 8601 strings with 'Z' suffix (e.g., '2024-01-01T00:00:00.000Z')
   * - Generated time series columns (date_from/date_to) contain VALUES data without 'Z' (e.g., '2024-01-01T00:00:00.000')
   *
   * @param value - Either '?' for bind parameters, a column identifier, or a SQL expression
   * @param includeZFormat - Whether format string should expect 'Z' suffix (true for filter params, false for series columns)
   * @returns Oracle SQL expression with appropriate bind placeholder or direct column reference
   */
  private toTimestampTz(value: string, includeZFormat: boolean): string {
    const format = includeZFormat ? OracleQuery.ORACLE_TZ_FORMAT_WITH_Z : OracleQuery.ORACLE_TZ_FORMAT_NO_Z;
    if (value === '?') {
      return `TO_TIMESTAMP_TZ(:"?", '${format}')`;
    }
    if (this.isIdentifierToken(value)) {
      // Column identifiers (e.g., date_from, date_to from generated time series) - use directly
      return `TO_TIMESTAMP_TZ(${value}, '${format}')`;
    }
    // SQL expressions or literals - embed directly in TO_TIMESTAMP_TZ call
    return `TO_TIMESTAMP_TZ(${value}, '${format}')`;
  }

  /**
   * "LIMIT" on Oracle is illegal
   * TODO replace with limitOffsetClause override
   */
  public groupByDimensionLimit() {
    const limitClause = this.rowLimit === null ? '' : ` FETCH NEXT ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000} ROWS ONLY`;
    const offsetClause = this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
    return `${offsetClause}${limitClause}`;
  }

  /**
   * "AS" for table aliasing on Oracle it's illegal
   */
  public get asSyntaxTable() {
    return '';
  }

  public get asSyntaxJoin() {
    return this.asSyntaxTable;
  }

  /**
   * Oracle doesn't support group by index,
   * using forSelect dimensions for grouping
   */
  public groupByClause() {
    // Only include dimensions that have select columns
    // Time dimensions without granularity return null from selectColumns()
    const dimensions = this.forSelect().filter((item: any) => (
      !!item.dimension && item.selectColumns && item.selectColumns()
    )) as BaseDimension[];
    if (!dimensions.length) {
      return '';
    }

    return ` GROUP BY ${dimensions.map(item => item.dimensionSql()).join(', ')}`;
  }

  public convertTz(field) {
    /**
     * TODO: add offset timezone
     */
    return field;
  }

  /**
   * Casts a value to Oracle DATE type using timezone-aware parsing.
   * For bind parameters ('?'), includes 'Z' suffix in format string.
   * For column identifiers (e.g., date_from/date_to from time series), omits 'Z'.
   *
   * @param value - Bind parameter placeholder '?', column identifier, or SQL expression
   */
  public dateTimeCast(value) {
    return `CAST(${this.toTimestampTz(value, value === '?')} AS DATE)`;
  }

  /**
   * Casts a value to Oracle TIMESTAMP WITH TIME ZONE.
   * For bind parameters ('?'), includes 'Z' suffix in format string.
   * For column identifiers (e.g., date_from/date_to from time series), omits 'Z'.
   *
   * @param value - Bind parameter placeholder '?', column identifier, or SQL expression
   */
  public timeStampCast(value) {
    return this.toTimestampTz(value, value === '?');
  }

  public timeStampParam(timeDimension) {
    return timeDimension.dateFieldType() === 'string' ? ':"?"' : this.timeStampCast('?');
  }

  public timeGroupedColumn(granularity, dimension) {
    if (!granularity) {
      return dimension;
    }
    return `TRUNC(${dimension}, '${GRANULARITY_VALUE[granularity]}')`;
  }

  /**
   * Oracle uses ADD_MONTHS for year/month/quarter intervals
   * and NUMTODSINTERVAL for day/hour/minute/second intervals
   */
  public addInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    // Handle year/month/quarter using ADD_MONTHS
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

    if (totalMonths !== 0) {
      res = `ADD_MONTHS(${res}, ${totalMonths})`;
    }

    // Handle day/hour/minute/second using NUMTODSINTERVAL
    if (intervalParsed.day) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.day}, 'DAY')`;
    }
    if (intervalParsed.hour) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.hour}, 'HOUR')`;
    }
    if (intervalParsed.minute) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.minute}, 'MINUTE')`;
    }
    if (intervalParsed.second) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.second}, 'SECOND')`;
    }

    return res;
  }

  /**
   * Oracle subtraction uses ADD_MONTHS with negative values
   * and subtracts NUMTODSINTERVAL for time units
   */
  public subtractInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    // Handle year/month/quarter using ADD_MONTHS with negative values
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

    if (totalMonths !== 0) {
      res = `ADD_MONTHS(${res}, -${totalMonths})`;
    }

    // Handle day/hour/minute/second using NUMTODSINTERVAL with subtraction
    if (intervalParsed.day) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.day}, 'DAY')`;
    }
    if (intervalParsed.hour) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.hour}, 'HOUR')`;
    }
    if (intervalParsed.minute) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.minute}, 'MINUTE')`;
    }
    if (intervalParsed.second) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.second}, 'SECOND')`;
    }

    return res;
  }

  public newFilter(filter) {
    return new OracleFilter(this, filter);
  }

  public unixTimestampSql() {
    // eslint-disable-next-line quotes
    return `((cast (systimestamp at time zone 'UTC' as date) - date '1970-01-01') * 86400)`;
  }

  public preAggregationTableName(cube, preAggregationName, skipSchema) {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 128) {
      throw new UserError(`Oracle can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }
}
