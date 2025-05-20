import inlection from 'inflection';
import moment from 'moment-timezone';
import { contains, join, map } from 'ramda';
import { FROM_PARTITION_RANGE, TO_PARTITION_RANGE } from '@cubejs-backend/shared';

import { BaseDimension } from './BaseDimension';
import type { BaseQuery } from './BaseQuery';

const DATE_OPERATORS = ['inDateRange', 'notInDateRange', 'onTheDate', 'beforeDate', 'beforeOrOnDate', 'afterDate', 'afterOrOnDate'];
const dateTimeLocalMsRegex = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d$/;
const dateTimeLocalURegex = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d\d\d\d$/;
const dateRegex = /^\d\d\d\d-\d\d-\d\d$/;

export class BaseFilter extends BaseDimension {
  public static readonly ALWAYS_TRUE: string = '1 = 1';

  public readonly measure: any;

  public readonly operator: any;

  public readonly values: any;

  public readonly camelizeOperator: any;

  public constructor(query: BaseQuery, filter: any) {
    super(query, filter.dimension);
    this.measure = filter.measure;

    this.operator = filter.operator;
    this.values = filter.values;
    this.camelizeOperator = inlection.camelize(this.operator).replace(
      /[A-Z]/,
      (c) => (c != null ? c : '').toLowerCase(),
    );
  }

  public filterToWhere() {
    if (this.camelizeOperator === 'measureFilter') {
      return this.measureFilterToWhere();
    }

    return this.conditionSql(this.measure ? this.query.measureSql(this) : this.query.dimensionSql(this));
  }

  public convertTzForRawTimeDimensionIfNeeded(sql) {
    return sql();
  }

  // Evaluates filters on measures to whole where statement in query
  // It used in drill-downs
  public measureFilterToWhere() {
    const measureDefinition = this.measureDefinition();
    if (measureDefinition.filters?.length || measureDefinition.drillFilters?.length) {
      return this.query.evaluateFiltersArray(
        (measureDefinition.filters || []).concat(measureDefinition.drillFilters || []),
        this.query.cubeEvaluator.cubeNameFromPath(this.measure)
      );
    } else {
      return null;
    }
  }

  public conditionSql(columnSql) {
    const operatorMethod = `${(this.camelizeOperator)}Where`;

    let sql = this[operatorMethod](columnSql);
    if (this.query.paramAllocator.hasParametersInSql(sql)) {
      return sql;
    }

    // TODO DEPRECATED: remove and replace with error
    // columnSql can contain `?` so allocate params first and then substitute columnSql
    // fallback implementation for drivers that still use `?` substitution
    sql = this[operatorMethod]('$$$COLUMN$$$');
    return this.query.paramAllocator.allocateParamsForQuestionString(sql, this.filterParams()).replace(/\$\$\$COLUMN\$\$\$/g, columnSql);
  }

  public measureDefinition() {
    return this.query.cubeEvaluator.measureByPath(this.measure);
  }

  public path() {
    if (this.measure) {
      return this.query.cubeEvaluator.parsePath('measures', this.measure);
    } else if (this.query.cubeEvaluator.isInstanceOfType('segments', this.dimension)) {
      return this.query.cubeEvaluator.parsePath('segments', this.dimension);
    } else {
      return this.query.cubeEvaluator.parsePath('dimensions', this.dimension);
    }
  }

  /**
   * BaseFilter inherits from BaseDimension while Filter may be measure-based !!
   */
  public override dateFieldType() {
    if (this.measure) {
      return this.measureDefinition().type; // There is no fieldType in measure, but it seems that it's enough
    } else {
      return this.dimensionDefinition().fieldType;
    }
  }

  public cube() {
    return this.query.cubeEvaluator.cubeFromPath(this.measure || this.dimension);
  }

  public definition() {
    return this.measure ? this.measureDefinition() : this.dimensionDefinition();
  }

  // noinspection JSMethodCanBeStatic
  public escapeWildcardChars(param) {
    return typeof param === 'string' ? param.replace(/([_%])/gi, '\\$1') : param;
  }

  public isWildcardOperator() {
    return this.camelizeOperator === 'contains' || this.camelizeOperator === 'notContains';
  }

  public filterParams() {
    if (this.isDateOperator()) {
      return [this.inDbTimeZoneDateFrom(this.values[0]), this.inDbTimeZoneDateTo(this.values[1])];
    }
    if (this.camelizeOperator === 'set' || this.camelizeOperator === 'notSet' || this.camelizeOperator === 'expressionEquals') {
      return [];
    }
    const params = this.valuesArray().filter(v => v != null);

    if (this.isWildcardOperator()) {
      return map(this.escapeWildcardChars, params);
    }

    return params;
  }

  public isDateOperator(): boolean {
    return contains(this.camelizeOperator, DATE_OPERATORS);
  }

  public valuesArray() {
    return Array.isArray(this.values) ? this.values : [this.values];
  }

  public valuesContainNull() {
    return this.valuesArray().indexOf(null) !== -1;
  }

  public castParameter() {
    return '?';
  }

  public firstParameter() {
    const params = this.filterParams();
    if (!params.length) {
      throw new Error('Expected one parameter but nothing found');
    }

    return this.allocateCastParam(params[0]);
  }

  public allocateCastParam(param) {
    return this.query.paramAllocator.allocateParamsForQuestionString(this.castParameter(), [param]);
  }

  public allocateTimestampParam(param) {
    return this.query.paramAllocator.allocateParamsForQuestionString(this.query.timeStampParam(this), [param]);
  }

  public allocateTimestampParams() {
    return this.filterParams().map((p, i) => {
      if (i > 1) {
        throw new Error(`Expected only 2 parameters for timestamp filter but got: ${this.filterParams()}`);
      }
      return this.allocateTimestampParam(p);
    });
  }

  public allParamsRepeat(basePart) {
    return this.filterParams().map(p => this.query.paramAllocator.allocateParamsForQuestionString(basePart, [p]));
  }

  public isArrayValues() {
    return Array.isArray(this.values) && this.values.length > 1;
  }

  public containsWhere(column) {
    return this.likeOr(column, false, 'contains');
  }

  public notContainsWhere(column) {
    return this.likeOr(column, true, 'contains');
  }

  /**
   * Returns SQL statement for the `startsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  public startsWithWhere(column) {
    return this.likeOr(column, false, 'starts');
  }

  /**
   * Returns SQL statement for the `notStartsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  public notStartsWithWhere(column) {
    return this.likeOr(column, true, 'starts');
  }

  /**
   * Returns SQL statement for the `endsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  public endsWithWhere(column) {
    return this.likeOr(column, false, 'ends');
  }

  /**
   * Returns SQL statement for the `endsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  public notEndsWithWhere(column) {
    return this.likeOr(column, true, 'ends');
  }

  /**
   * Returns SQL filter statement (union with the logical OR) for the
   * provided parameters.
   * @param {string} column Column name.
   * @param {boolean} not Flag to build NOT LIKE statement.
   * @param {string} type Type of the condition (i.e. contains/
   * startsWith/endsWith).
   * @returns string
   */
  public likeOr(column, not, type) {
    type = type || 'contains';
    return `${join(not ? ' AND ' : ' OR ', this.filterParams().map(
      p => this.likeIgnoreCase(column, not, p, type)
    ))}${this.orIsNullCheck(column, not)}`;
  }

  /**
   * Returns SQL LIKE statement for specified parameters.
   * @param {string} column Column name.
   * @param {boolean} not Flag to build NOT LIKE statement.
   * @param {*} param Value for statement.
   * @param {string} type Type of the condition (i.e. contains/startsWith/endsWith).
   * @returns string
   */
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} ILIKE ${p}${this.allocateParam(param)}${s}`;
  }

  public orIsNullCheck(column, not) {
    return `${this.shouldAddOrIsNull(not) ? ` OR ${column} IS NULL` : ''}`;
  }

  public shouldAddOrIsNull(not) {
    return not ? !this.valuesContainNull() : this.valuesContainNull();
  }

  public allocateParam(param) {
    return this.query.paramAllocator.allocateParam(param);
  }

  public equalsWhere(column) {
    if (this.isArrayValues()) {
      return this.inWhere(column);
    }

    if (this.valuesContainNull()) {
      return this.notSetWhere(column);
    }

    return `${column} = ${this.firstParameter()}${this.orIsNullCheck(column, false)}`;
  }

  public inPlaceholders() {
    return `(${join(', ', this.filterParams().map(p => this.allocateCastParam(p)))})`;
  }

  public inWhere(column) {
    return `${column} IN ${this.inPlaceholders()}${this.orIsNullCheck(column, false)}`;
  }

  public notEqualsWhere(column) {
    if (this.isArrayValues()) {
      return this.notInWhere(column);
    }

    if (this.valuesContainNull()) {
      return this.setWhere(column);
    }

    return `${column} <> ${this.firstParameter()}${this.orIsNullCheck(column, true)}`;
  }

  public notInWhere(column) {
    return `${column} NOT IN ${this.inPlaceholders()}${this.orIsNullCheck(column, true)}`;
  }

  public setWhere(column) {
    return `${column} IS NOT NULL`;
  }

  public notSetWhere(column) {
    return `${column} IS NULL`;
  }

  public gtWhere(column) {
    return `${column} > ${this.firstParameter()}`;
  }

  public gteWhere(column) {
    return `${column} >= ${this.firstParameter()}`;
  }

  public ltWhere(column) {
    return `${column} < ${this.firstParameter()}`;
  }

  public lteWhere(column) {
    return `${column} <= ${this.firstParameter()}`;
  }

  public expressionEqualsWhere(column) {
    return `${column} = ${this.values[0]}`;
  }

  public inDateRangeWhere(column) {
    const [from, to] = this.allocateTimestampParams();
    if (!from || !to) {
      return BaseFilter.ALWAYS_TRUE;
    }
    return this.query.timeRangeFilter(column, from, to);
  }

  public notInDateRangeWhere(column) {
    const [from, to] = this.allocateTimestampParams();
    if (!from || !to) {
      return BaseFilter.ALWAYS_TRUE;
    }
    return this.query.timeNotInRangeFilter(column, from, to);
  }

  public onTheDateWhere(column) {
    const [from, to] = this.allocateTimestampParams();
    if (!from || !to) {
      return BaseFilter.ALWAYS_TRUE;
    }
    return this.query.timeRangeFilter(column, from, to);
  }

  public beforeDateWhere(column) {
    const [before] = this.allocateTimestampParams();
    if (!before) {
      return BaseFilter.ALWAYS_TRUE;
    }
    return this.query.beforeDateFilter(column, before);
  }

  public beforeOrOnDateWhere(column) {
    const [before] = this.allocateTimestampParams();
    if (!before) {
      return BaseFilter.ALWAYS_TRUE;
    }
    return this.query.beforeOrOnDateFilter(column, before);
  }

  public afterDateWhere(column) {
    const [after] = this.allocateTimestampParams();
    if (!after) {
      return BaseFilter.ALWAYS_TRUE;
    }
    return this.query.afterDateFilter(column, after);
  }

  public afterOrOnDateWhere(column) {
    const [after] = this.allocateTimestampParams();
    if (!after) {
      return BaseFilter.ALWAYS_TRUE;
    }
    return this.query.afterOrOnDateFilter(column, after);
  }

  public formatFromDate(date: string) {
    if (date) {
      if (this.query.timestampPrecision() === 3) {
        if (date.match(dateTimeLocalMsRegex)) {
          return date;
        }
      } else if (this.query.timestampPrecision() === 6) {
        if (date.length === 23 && date.match(dateTimeLocalMsRegex)) {
          return `${date}000`;
        } else if (date.length === 26 && date.match(dateTimeLocalURegex)) {
          return date;
        }
      } else {
        throw new Error(`Unsupported timestamp precision: ${this.query.timestampPrecision()}`);
      }

      if (date.match(dateRegex)) {
        return `${date}T00:00:00.${'0'.repeat(this.query.timestampPrecision())}`;
      }
    }

    if (!date) {
      return moment.tz(date, this.query.timezone).format(`YYYY-MM-DDT00:00:00.${'0'.repeat(this.query.timestampPrecision())}`);
    }

    return moment.tz(date, this.query.timezone).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }

  public inDbTimeZoneDateFrom(date) {
    if (date && (date === FROM_PARTITION_RANGE || date === TO_PARTITION_RANGE)) {
      return date;
    }

    return this.query.inDbTimeZone(this.formatFromDate(date));
  }

  public formatToDate(date: string) {
    if (date) {
      if (this.query.timestampPrecision() === 3) {
        if (date.match(dateTimeLocalMsRegex)) {
          return date;
        }
      } else if (this.query.timestampPrecision() === 6) {
        if (date.length === 23 && date.match(dateTimeLocalMsRegex)) {
          if (date.endsWith('.999')) {
            return `${date}999`;
          }

          return `${date}000`;
        } else if (date.length === 26 && date.match(dateTimeLocalURegex)) {
          return date;
        }
      } else {
        throw new Error(`Unsupported timestamp precision: ${this.query.timestampPrecision()}`);
      }

      if (date.match(dateRegex)) {
        return `${date}T23:59:59.${'9'.repeat(this.query.timestampPrecision())}`;
      }
    }

    if (!date) {
      return moment.tz(date, this.query.timezone).format(`YYYY-MM-DDT23:59:59.${'9'.repeat(this.query.timestampPrecision())}`);
    }

    return moment.tz(date, this.query.timezone).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }

  public inDbTimeZoneDateTo(date) {
    if (date && (date === FROM_PARTITION_RANGE || date === TO_PARTITION_RANGE)) {
      return date;
    }
    return this.query.inDbTimeZone(this.formatToDate(date));
  }

  public formattedDateRange() {
    return [this.formatFromDate(this.values[0]), this.formatToDate(this.values[1])];
  }

  public unescapedAliasName() {
    return this.query.aliasName(this.measure || this.dimension);
  }
}
