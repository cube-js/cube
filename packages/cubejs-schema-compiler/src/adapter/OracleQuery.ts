import { BaseQuery } from './BaseQuery';
import { UserError } from '../compiler/UserError';
import { BaseDimension } from './BaseDimension';
import { BaseTimeDimension } from './BaseTimeDimension';
import { ParamAllocator } from './ParamAllocator';
import { OracleParamAllocator } from './OracleParamAllocator';
import { OracleFilter } from './OracleFilter';
import { BaseFilter } from './BaseFilter';

const GRANULARITY_VALUE = {
  day: 'DD',
  week: 'IW',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  quarter: 'Q',
  year: 'YYYY'
};

export class OracleQuery extends BaseQuery {
    bindPosition = 0;

    /**
     * "LIMIT" on Oracle it's illegal
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
     * @description Need to add in 'AS' keyword in this case vs. PG SQL
     * (This is different than using the AS keyword in PG SQL for table aliasing)
     * @param cube
     * @param preAggregation
     * @param tableName
     */
    public preAggregationLoadSql(cube: string, preAggregation: unknown, tableName: string) {
      const sqlAndParams = this.preAggregationSql(cube, preAggregation);
      return [`CREATE TABLE ${tableName} AS ${sqlAndParams[0]}`, sqlAndParams[1]];
    }

    /**
     * @description Oracle doesn't support group by index,
     * using forSelect dimensions for grouping
     */
    public groupByClause() {
      const dimensions = this.forSelect().filter((item: any) => !!item.dimension) as BaseDimension[];
      if (!dimensions.length) {
        return '';
      }

      return ` GROUP BY ${dimensions.map(item => item.dimensionSql()).join(', ')}`;
    }

    /**
     * @description Oracle SQL to add a TZ to a date time
     * @param field
     */
    public convertTz(field: string) {
      return `${field} AT TIME ZONE '${this.timezone}'`;
    }

    /**
     * @description Takes an ISO-8601 formatted UTC date-time string and converts to TIMESTAMP
     * @param value
     */
    public dateTimeCast(value: string) {
      return `TO_UTC_TIMESTAMP_TZ(${value})`; // . + 3 digits + TZ
    }

    /**
     * @description Handles issues with sub-query parameter substitution
     * @param expressionParams
     */
    public newParamAllocator(expressionParams: unknown[]): ParamAllocator {
      return new OracleParamAllocator(expressionParams);
    }

    public timeStampCast(value: string) {
      return this.dateTimeCast(value);
    }

    public timeStampParam(timeDimension: BaseDimension | BaseTimeDimension) {
      return this.timeStampCast('?');
    }

    /**
     * @description adds oracle syntax to extract a part of a datetime
     * @param granularity
     * @param dimension
     */
    public timeGroupedColumn(granularity: string, dimension: string) {
      if (!granularity) {
        return dimension;
      }
      return `TRUNC(${dimension}, '${GRANULARITY_VALUE[granularity]}')`;
    }

    public newFilter(filter: BaseFilter) {
      return new OracleFilter(this, filter);
    }

    public nowTimestampSql(): string {
      return 'SYSTIMESTAMP';
    }

    public unixTimestampSql() {
      return '( CAST(SYSTIMESTAMP AT TIME ZONE \'UTC\' as DATE) - DATE \'1970-01-01\' ) * 86400';
    }

    public preAggregationTableName(cube: string, preAggregationName: string, skipSchema: string) {
      const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
      if (name.length > 128) {
        throw new UserError(`Oracle can not work with table names that are longer than 128 bytes. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
      }
      return name;
    }

    public refreshKeySelect(sql: string) {
      return `SELECT ${sql} as refresh_key FROM DUAL`;
    }
}
