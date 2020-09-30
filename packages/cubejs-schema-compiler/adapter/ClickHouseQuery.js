const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');
const UserError = require('../compiler/UserError');

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
  likeIgnoreCase(column, not) {
    return `lower(${column}) ${not ? 'NOT' : ''} LIKE CONCAT('%', lower(?), '%')`;
  }

  castParameter() {
    if (this.measure || this.definition().type === 'number') {
      // TODO here can be measure type of string actually
      return 'toFloat64(?)';
    }
    return '?';
  }
}

class ClickHouseQuery extends BaseQuery {
  newFilter(filter) {
    return new ClickHouseFilter(this, filter);
  }

  escapeColumnName(name) {
    return `\`${name}\``;
  }

  convertTz(field) {
    //
    // field yields a Date or a DateTime so add in the extra toDateTime to support the Date case
    //
    // https://clickhouse.yandex/docs/en/data_types/datetime/
    // https://clickhouse.yandex/docs/en/query_language/functions/date_time_functions/
    //
    //
    return `toTimeZone(toDateTime(${field}), '${this.timezone}')`;
  }

  timeGroupedColumn(granularity, dimension) {
    if (granularity === 'week') {
      return `toDateTime(toMonday(${dimension}, '${this.timezone}'), '${this.timezone}')`;
    } else {
      const interval = GRANULARITY_TO_INTERVAL[granularity];
      return `toDateTime(${granularity === 'second' ? 'toDateTime' : `toStartOf${interval}`}(${dimension}, '${this.timezone}'), '${this.timezone}')`;
    }
  }

  calcInterval(operation, date, interval) {
    const [intervalValue, intervalUnit] = interval.split(" ");
    // eslint-disable-next-line prefer-template
    const fn = operation + intervalUnit[0].toUpperCase() + intervalUnit.substring(1) + "s";
    return `${fn}(${date}, ${intervalValue})`;
  }

  subtractInterval(date, interval) {
    return this.calcInterval("subtract", date, interval);
  }

  addInterval(date, interval) {
    return this.calcInterval("add", date, interval);
  }

  timeStampCast(value) {
    // value yields a string formatted in ISO8601, so this function returns a expression to parse a string to a DateTime

    //
    // ClickHouse provides toDateTime which expects dates in UTC in format YYYY-MM-DD HH:MM:SS
    //
    // However parseDateTimeBestEffort works with ISO8601
    //
    return `parseDateTimeBestEffort(${value})`;
  }

  dateTimeCast(value) {
    // value yields a string formatted in ISO8601, so this function returns a expression to parse a string to a DateTime

    //
    // ClickHouse provides toDateTime which expects dates in UTC in format YYYY-MM-DD HH:MM:SS
    //
    // However parseDateTimeBestEffort works with ISO8601
    //
    return `parseDateTimeBestEffort(${value})`;
  }

  getFieldAlias(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let field;

    field = this.dimensionsForSelect().find(
      d => equalIgnoreCase(d.dimension, id)
    );

    if (field) {
      return field.aliasName();
    }

    field = this.measures.find(
      d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id)
    );

    if (field) {
      return field.aliasName();
    }

    return null;
  }

  orderHashToString(hash) {
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

  groupByClause() {
    //
    // ClickHouse doesn't support group by index column, so map these to the alias names
    //

    const names = this.dimensionAliasNames();
    return names.length ? ` GROUP BY ${names.join(', ')}` : '';
  }

  primaryKeyCount(cubeName, distinct) {
    const primaryKeySql = this.primaryKeySql(this.cubeEvaluator.primaryKeys[cubeName], cubeName);
    if (distinct) {
      return `uniqExact(${primaryKeySql})`;
    } else {
      return `count(${primaryKeySql})`;
    }
  }

  seriesSql(timeDimension) {
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

    const datesFrom = [];
    const datesTo = [];
    timeDimension.timeSeries().forEach(([from, to]) => {
      datesFrom.push(from);
      datesTo.push(to);
    });
    return `SELECT parseDateTimeBestEffort(arrayJoin(['${datesFrom.join("','")}'])) as date_from, parseDateTimeBestEffort(arrayJoin(['${datesTo.join("','")}'])) as date_to`;
  }

  concatStringsSql(strings) {
    // eslint-disable-next-line prefer-template
    return "toString(" + strings.join(") || toString(") + ")";
  }

  unixTimestampSql() {
    return `toUnixTimestamp(${this.nowTimestampSql()})`;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    if (!preAggregation.indexes) {
      throw new UserError(`ClickHouse doesn't support pre-aggregations without indexes`);
    }
    const firstIndexName = Object.keys(preAggregation.indexes)[0];
    const indexColumns = this.evaluateIndexColumns(cube, preAggregation.indexes[firstIndexName]);
    return [`CREATE TABLE ${tableName} ENGINE = MergeTree() ORDER BY (${indexColumns.join(', ')}) ${this.asSyntaxTable} ${sqlAndParams[0]}`, sqlAndParams[1]];
  }

  createIndexSql(indexName, tableName, escapedColumns) {
    return `ALTER TABLE ${tableName} ADD INDEX ${indexName} (${escapedColumns.join(', ')}) TYPE minmax GRANULARITY 1`;
  }
}

module.exports = ClickHouseQuery;
