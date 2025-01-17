import { parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import { BaseTimeDimension } from './BaseTimeDimension';

const GRANULARITY_TO_INTERVAL = {
  day: 'Day',
  hour: 'Hour',
  minute: 'Minute',
  second: 'Second',
  month: 'Month',
  quarter: 'Quarter',
  year: 'Year',
};

class ClickHouseFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `lower(${column}) ${not ? 'NOT' : ''} LIKE CONCAT('${p}', lower(${this.allocateParam(param)}), '${s}')`;
  }

  public castParameter() {
    if (this.measure || this.definition().type === 'number') {
      // TODO here can be measure type of string actually
      return 'toFloat64(?)';
    }
    return '?';
  }
}

export class ClickHouseQuery extends BaseQuery {
  public newFilter(filter) {
    return new ClickHouseFilter(this, filter);
  }

  public escapeColumnName(name) {
    return `\`${name}\``;
  }

  public convertTz(field) {
    //
    // field yields a Date or a DateTime so add in the extra toDateTime64 to support the Date case
    //
    // https://clickhouse.yandex/docs/en/data_types/datetime/
    // https://clickhouse.yandex/docs/en/query_language/functions/date_time_functions/
    //
    //
    return `toTimeZone(toDateTime64(${field}, 0), '${this.timezone}')`;
  }

  public timeGroupedColumn(granularity, dimension) {
    if (granularity === 'week') {
      return `toDateTime64(toMonday(${dimension}, '${this.timezone}'), 0, '${this.timezone}')`;
    } else {
      const interval = GRANULARITY_TO_INTERVAL[granularity];
      const toDateTime64 = `toDateTime64(${dimension}, 0, '${this.timezone}')`;
      const toStartOfInterval = `toStartOf${interval}(${dimension}, '${this.timezone}')`;
      const internalConversion = granularity === 'second' ? toDateTime64 : toStartOfInterval;
      return `toDateTime64(${internalConversion}, 0, '${this.timezone}')`;
    }
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const intervalFormatted = this.formatInterval(interval);
    const timeUnit = this.diffTimeUnitForInterval(interval);
    const beginOfTime = 'fromUnixTimestamp(0)';

    return `date_add(${timeUnit},
        FLOOR(
          date_diff(${timeUnit}, ${this.dateTimeCast(`'${origin}'`)}, ${source}) /
          date_diff(${timeUnit}, ${beginOfTime}, ${beginOfTime} + ${intervalFormatted})
        ) * date_diff(${timeUnit}, ${beginOfTime}, ${beginOfTime} + ${intervalFormatted}),
        ${this.dateTimeCast(`'${origin}'`)}
    )`;
  }

  public subtractInterval(date: string, interval: string): string {
    return `subDate(${date}, ${this.formatInterval(interval)})`;
  }

  public addInterval(date: string, interval: string): string {
    return `addDate(${date}, ${this.formatInterval(interval)})`;
  }

  /**
   * The input interval with (possible) plural units, like "2 years", "3 months", "4 weeks", "5 days"...
   * will be converted to ClickHouse form of sum of single intervals.
   * @see https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/interval
   */
  private formatInterval(interval: string): string {
    const intervalParsed = parseSqlInterval(interval);

    return Object.entries(intervalParsed)
      .map(([key, value]) => `INTERVAL ${value} ${key.toUpperCase()}`)
      .join(' + ');
  }

  public timeStampCast(value: string): string {
    return this.dateTimeCast(value);
  }

  public dateTimeCast(value: string): string {
    // value yields a string formatted in ISO8601, so this function returns a expression to parse a string to a DateTime
    // ClickHouse provides toDateTime which expects dates in UTC in format YYYY-MM-DD HH:MM:SS
    // However parseDateTimeBestEffort works with ISO8601
    //
    return `parseDateTimeBestEffort(${value})`;
  }

  public dimensionsJoinCondition(leftAlias, rightAlias) {
    const dimensionAliases = this.dimensionAliasNames();
    if (!dimensionAliases.length) {
      return '1 = 1';
    }
    return dimensionAliases
      .map(alias => `(assumeNotNull(${leftAlias}.${alias}) = assumeNotNull(${rightAlias}.${alias}))`)
      .join(' AND ');
  }

  public getFieldAlias(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let field;

    field = this.dimensionsForSelect().find(
      d => equalIgnoreCase(d.dimension, id),
    );

    if (field) {
      return field.aliasName();
    }

    field = this.measures.find(
      d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id),
    );

    if (field) {
      return field.aliasName();
    }

    return null;
  }

  public orderHashToString(hash) {
    //
    // ClickHouse doesn't support order by index column, so map these to the alias names
    //

    if (!hash || !hash.id) {
      return null;
    }

    const fieldAlias = this.getFieldAlias(hash.id);

    if (fieldAlias === null) {
      return null;
    }

    const direction = hash.desc ? 'DESC' : 'ASC';
    return `${fieldAlias} ${direction}`;
  }

  public groupByClause() {
    if (this.ungrouped) {
      return '';
    }
    //
    // ClickHouse doesn't support group by index column, so map these to the alias names
    //

    const names = this.dimensionAliasNames();
    return names.length ? ` GROUP BY ${names.join(', ')}` : '';
  }

  public primaryKeyCount(cubeName, distinct) {
    const primaryKeys = this.cubeEvaluator.primaryKeys[cubeName];
    const primaryKeySql = primaryKeys.length > 1 ?
      this.concatStringsSql(primaryKeys.map((pk) => this.castToString(this.primaryKeySql(pk, cubeName)))) :
      this.primaryKeySql(primaryKeys[0], cubeName);
    if (distinct) {
      return `uniqExact(${primaryKeySql})`;
    } else {
      return `count(${primaryKeySql})`;
    }
  }

  public castToString(sql) {
    return `CAST(${sql} as String)`;
  }

  public seriesSql(timeDimension: BaseTimeDimension) {
    /*
    postgres uses :

    SELECT parseDateTimeBestEffort(date_from), parseDateTimeBestEffort(date_to) FROM
    (
        VALUES
          ('2017-01-01T00:00:00.000', '2017-01-01T23:59:59.999'),
          ('2017-01-02T00:00:00.000', '2017-01-02T23:59:59.999'),
          ('2017-01-03T00:00:00.000', '2017-01-03T23:59:59.999'),
          ('2017-01-04T00:00:00.000', '2017-01-04T23:59:59.999'),
          ('2017-01-05T00:00:00.000', '2017-01-05T23:59:59.999'),
          ('2017-01-06T00:00:00.000', '2017-01-06T23:59:59.999'),
          ('2017-01-07T00:00:00.000', '2017-01-07T23:59:59.999'),
          ('2017-01-08T00:00:00.000', '2017-01-08T23:59:59.999'),
          ('2017-01-09T00:00:00.000', '2017-01-09T23:59:59.999'),
          ('2017-01-10T00:00:00.000', '2017-01-10T23:59:59.999')
        ) AS dates (date_from, date_to)
      ) AS `visitors.created_at_series`

    */
    /*

   ClickHouse uses :

     select
      parseDateTimeBestEffort(arrayJoin(['2017-01-01T00:00:00.000','2017-01-02T00:00:00.000'])) as date_from,
      parseDateTimeBestEffort(arrayJoin(['2017-01-01T23:59:59.999','2017-01-02T23:59:59.999'])) as date_to
      ...
   )
   */

    const datesFrom: string[] = [];
    const datesTo: string[] = [];

    timeDimension.timeSeries().forEach(([from, to]) => {
      datesFrom.push(from);
      datesTo.push(to);
    });

    return `SELECT parseDateTimeBestEffort(arrayJoin(['${datesFrom.join('\',\'')}'])) as date_from, parseDateTimeBestEffort(arrayJoin(['${datesTo.join('\',\'')}'])) as date_to`;
  }

  public concatStringsSql(strings) {
    // eslint-disable-next-line prefer-template
    return 'toString(' + strings.join(') || toString(') + ')';
  }

  public unixTimestampSql() {
    return `toUnixTimestamp(${this.nowTimestampSql()})`;
  }

  public preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    if (!preAggregation.indexes) {
      throw new UserError('ClickHouse doesn\'t support pre-aggregations without indexes');
    }
    const firstIndexName = Object.keys(preAggregation.indexes)[0];
    const indexColumns = this.evaluateIndexColumns(cube, preAggregation.indexes[firstIndexName]);
    return [`CREATE TABLE ${tableName} ENGINE = MergeTree() ORDER BY (${indexColumns.join(', ')}) ${this.asSyntaxTable} ${sqlAndParams[0]}`, sqlAndParams[1]];
  }

  public countDistinctApprox(sql: string): string {
    return `uniq(${sql})`;
  }

  public createIndexSql(indexName, tableName, escapedColumns) {
    return `ALTER TABLE ${tableName} ADD INDEX ${indexName} (${escapedColumns.join(', ')}) TYPE minmax GRANULARITY 1`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    // TODO: Introduce additional filter in jinja? or parseDateTimeBestEffort?
    // https://github.com/ClickHouse/ClickHouse/issues/19351
    templates.expressions.timestamp_literal = 'parseDateTimeBestEffort(\'{{ value }}\')';
    delete templates.expressions.like_escape;
    templates.quotes.identifiers = '`';
    templates.quotes.escape = '\\`';
    templates.types.boolean = 'BOOL';
    templates.types.timestamp = 'DATETIME';
    delete templates.types.time;
    // ClickHouse intervals have a distinct type for each granularity
    delete templates.types.interval;
    delete templates.types.binary;
    return templates;
  }
}
