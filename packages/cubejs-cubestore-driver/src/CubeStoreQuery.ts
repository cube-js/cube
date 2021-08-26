import moment from 'moment-timezone';
import { BaseFilter, BaseQuery, UserError, BaseMeasure } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  quarter: 'quarter',
  year: 'year'
};

class CubeStoreFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ${this.allocateParam(param)}, '%')`;
  }
}

type RollingWindow = {
  trailing?: string | 'unbounded';
  leading?: string | 'unbounded';
  offset?: 'start' | 'end';
};

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

  public timestampFormat() {
    return moment.HTML5_FMT.DATETIME_LOCAL_MS;
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
    return `merge(${sql})`;
  }

  public hllCardinalityMerge(sql) {
    return `cardinality(merge(${sql}))`;
  }

  public countDistinctApprox(sql) {
    // TODO: We should throw an error, but this gets called even when only `hllMerge` result is used.
    return `approx_distinct_is_unsupported_in_cubestore(${sql}))`;
  }

  public regularAndTimeSeriesRollupQuery(
    regularMeasures: BaseMeasure[],
    multipliedMeasures: BaseMeasure[],
    cumulativeMeasures: Array<[boolean, BaseMeasure]>,
    preAggregationForQuery: any
  ) {
    if (!cumulativeMeasures.length) {
      return super.regularAndTimeSeriesRollupQuery(regularMeasures, multipliedMeasures, cumulativeMeasures, preAggregationForQuery);
    }
    const cumulativeMeasuresWithoutMultiplied = cumulativeMeasures.map(([multiplied, measure]) => measure);
    const allMeasures = regularMeasures.concat(multipliedMeasures).concat(
      cumulativeMeasuresWithoutMultiplied
    );
    const timeDimension = this.timeDimensions.find(d => d.granularity);
    const baseQueryAlias = this.cubeAlias('base');
    const maxRollingWindow = cumulativeMeasuresWithoutMultiplied.reduce((a, b) => this.maxRollingWindow(a, b.rollingWindowDefinition()), <RollingWindow>{});
    const commonDateCondition =
      this.rollingWindowDateJoinCondition(maxRollingWindow.trailing, maxRollingWindow.leading, maxRollingWindow.offset);
    const filters = this.segments.concat(this.filters).concat(
      timeDimension?.dateRange && this.dateFromStartToEndConditionSql(commonDateCondition, true, true) || []
    );
    const rollupGranularity = this.preAggregations?.castGranularity(preAggregationForQuery.preAggregation.granularity) || 'day';
    return this.evaluateSymbolSqlWithContext(
      () => this.overTimeSeriesSelectRollup(
        cumulativeMeasuresWithoutMultiplied,
        regularMeasures.concat(multipliedMeasures),
        this.preAggregations?.rollupPreAggregation(preAggregationForQuery, allMeasures, false, filters),
        baseQueryAlias,
        timeDimension
      ),
      {
        rollupQuery: true,
        rollupGranularity,
      }
    );
  }

  public overTimeSeriesSelectRollup(cumulativeMeasures, otherMeasures, baseQuery, baseQueryAlias, timeDimension) {
    const rollingWindowClause = timeDimension &&
      ` ROLLING_WINDOW DIMENSION ${timeDimension.aliasName()} FROM ${this.timeGroupedColumn(timeDimension.granularity, timeDimension.localDateTimeFromOrBuildRangeParam())} TO ${this.timeGroupedColumn(timeDimension.granularity, timeDimension.localDateTimeToOrBuildRangeParam())} EVERY INTERVAL '1 ${timeDimension.granularity}'` || '';
    const forSelect = this.overTimeSeriesForSelectRollup(cumulativeMeasures, otherMeasures, timeDimension);
    return `SELECT ${forSelect} FROM (${baseQuery}) ${baseQueryAlias}${rollingWindowClause}${this.groupByClause()}`;
  }

  public toInterval(interval) {
    if (interval === 'unbounded') {
      return 'UNBOUNDED';
    } else {
      return `INTERVAL '${interval}'`;
    }
  }

  // TODO offset
  public maxRollingWindow(a: RollingWindow, b: RollingWindow): RollingWindow {
    let trailing;
    if (a.trailing === 'unbounded' || b.trailing === 'unbounded') {
      trailing = 'unbounded';
    } else if (!a.trailing) {
      trailing = b.trailing;
    } else if (!b.trailing) {
      trailing = a.trailing;
    } else {
      trailing = this.parseSecondDuration(a.trailing) > this.parseSecondDuration(b.trailing) ? a.trailing : b.trailing;
    }

    let leading;
    if (a.leading === 'unbounded' || b.leading === 'unbounded') {
      leading = 'unbounded';
    } else if (!a.leading) {
      leading = b.leading;
    } else if (!b.leading) {
      leading = a.leading;
    } else {
      leading = this.parseSecondDuration(a.leading) > this.parseSecondDuration(b.leading) ? a.leading : b.leading;
    }

    return {
      trailing, leading
    };
  }

  public overTimeSeriesForSelectRollup(cumulativeMeasures, otherMeasures, timeDimension) {
    const cumulativeSelectColumns = cumulativeMeasures.map(m => {
      if (timeDimension) {
        const measureSql = m.cumulativeMeasureSql();
        const rollingWindow = m.rollingWindowDefinition();
        const preceding = rollingWindow.trailing ? `${this.toInterval(rollingWindow.trailing)} PRECEDING` : '';
        const following = rollingWindow.leading ? `${this.toInterval(rollingWindow.leading)} FOLLOWING` : '';
        // TODO OFFSET ${rollingWindow.offset || 'END'}
        return [`ROLLING(${measureSql} ${preceding && following ? 'RANGE BETWEEN ' : 'RANGE '}${preceding}${preceding && following ? ' ' : ''}${following}) ${m.aliasName()}`];
      } else {
        const dateJoinCondition = m.dateJoinCondition();
        const conditionFn = this.dateFromStartToEndConditionSql(dateJoinCondition, true, true)[0];
        return this.evaluateSymbolSqlWithContext(
          () => {
            const aliasName = m.aliasName();
            const measureSql = this.aggregateOnGroupedColumn(
              m.measureDefinition(),
              aliasName,
              true,
              m.measure
            );
            return [`${measureSql} ${aliasName}`];
          },
          {
            cumulativeMeasureFilters: { [m.measure]: conditionFn }
          }
        );
      }
    });
    const otherSelectColumns = otherMeasures.map(m => this.evaluateSymbolSqlWithContext(
      () => {
        const aliasName = m.aliasName();
        const measureSql = this.aggregateOnGroupedColumn(
          m.measureDefinition(),
          aliasName,
          true,
          m.measure
        );
        return [`${measureSql} ${aliasName}`];
      },
      {
        cumulativeMeasureFilters: { [m.measure]: timeDimension }
      }
    ));
    return this.dimensions.concat(this.timeDimensions.filter(d => d.granularity)).map(s => s.cumulativeSelectColumns()).concat(
      cumulativeSelectColumns.concat(otherSelectColumns),
    ).filter(c => !!c)
      .join(', ');
  }
}
