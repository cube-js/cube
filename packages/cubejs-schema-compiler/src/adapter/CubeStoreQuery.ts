import moment from 'moment-timezone';
import { parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { BaseMeasure } from './BaseMeasure';

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
  public likeIgnoreCase(column, not, param, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} ILIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
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
    return `CAST(${value} as TIMESTAMP)`;
  }

  public timestampFormat() {
    return 'YYYY-MM-DDTHH:mm:ss.SSS';
  }

  public dateTimeCast(value) {
    return `to_timestamp(${value})`;
  }

  public subtractInterval(date: string, interval: string) {
    return `DATE_SUB(${date}, INTERVAL ${this.formatInterval(interval)})`;
  }

  public addInterval(date: string, interval: string) {
    return `DATE_ADD(${date}, INTERVAL ${this.formatInterval(interval)})`;
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    return `DATE_BIN(INTERVAL ${this.formatInterval(interval)}, ${this.dateTimeCast(source)}, ${this.dateTimeCast(`'${origin}'`)})`;
  }

  /**
   * The input interval with (possible) plural units, like "2 years", "3 months", "4 weeks", "5 days"...
   * will be converted to CubeStore (DataFusion) dialect.
   */
  private formatInterval(interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    const intKeys = Object.keys(intervalParsed).length;

    if (intervalParsed.year && intKeys === 1) {
      return `'${intervalParsed.year} YEAR'`;
    } else if (intervalParsed.year && intervalParsed.month && intKeys === 2) {
      return `'${intervalParsed.year} YEAR ${intervalParsed.month} MONTH'`;
    } else if (intervalParsed.year && intervalParsed.month && intervalParsed.quarter && intKeys === 3) {
      return `'${intervalParsed.year} YEAR ${intervalParsed.quarter} QUARTER ${intervalParsed.month} MONTH'`;
    } else if (intervalParsed.quarter && intKeys === 1) {
      return `'${intervalParsed.quarter} QUARTER'`;
    } else if (intervalParsed.quarter && intervalParsed.month && intKeys === 2) {
      return `'${intervalParsed.quarter} QUARTER ${intervalParsed.month} MONTH'`;
    } else if (intervalParsed.month && intKeys === 1) {
      return `'${intervalParsed.month} MONTH'`;
    } else if (intervalParsed.week && intKeys === 1) {
      return `'${intervalParsed.week} WEEK'`;
    } else if (intervalParsed.week && intervalParsed.day && intKeys === 2) {
      return `'${intervalParsed.week} WEEK ${intervalParsed.day} DAY'`;
    } else if (intervalParsed.week && intervalParsed.day && intervalParsed.hour && intKeys === 3) {
      return `'${intervalParsed.week} WEEK ${intervalParsed.day} DAY ${intervalParsed.hour} HOUR'`;
    } else if (intervalParsed.week && intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intKeys === 4) {
      return `'${intervalParsed.week} WEEK ${intervalParsed.day} DAY ${intervalParsed.hour} HOUR ${intervalParsed.minute} MINUTE'`;
    } else if (intervalParsed.week && intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 5) {
      return `'${intervalParsed.week} WEEK ${intervalParsed.day} DAY ${intervalParsed.hour} HOUR ${intervalParsed.minute} MINUTE ${intervalParsed.second} SECOND'`;
    } else if (intervalParsed.day && intKeys === 1) {
      return `'${intervalParsed.day} DAY'`;
    } else if (intervalParsed.day && intervalParsed.hour && intKeys === 2) {
      return `'${intervalParsed.day} DAY ${intervalParsed.hour} HOUR'`;
    } else if (intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intKeys === 3) {
      return `'${intervalParsed.day} DAY ${intervalParsed.hour} HOUR ${intervalParsed.minute} MINUTE'`;
    } else if (intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 4) {
      return `'${intervalParsed.day} DAY ${intervalParsed.hour} HOUR ${intervalParsed.minute} MINUTE ${intervalParsed.second} SECOND'`;
    } else if (intervalParsed.hour && intKeys === 1) {
      return `'${intervalParsed.hour} HOUR'`;
    } else if (intervalParsed.hour && intervalParsed.minute && intKeys === 2) {
      return `'${intervalParsed.hour} HOUR ${intervalParsed.minute} MINUTE'`;
    } else if (intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 3) {
      return `'${intervalParsed.hour} HOUR ${intervalParsed.minute} MINUTE ${intervalParsed.second} SECOND'`;
    } else if (intervalParsed.minute && intKeys === 1) {
      return `'${intervalParsed.minute} MINUTE'`;
    } else if (intervalParsed.minute && intervalParsed.second && intKeys === 2) {
      return `'${intervalParsed.minute} MINUTE ${intervalParsed.second} SECOND'`;
    }

    // No need to support microseconds.

    throw new Error(`Cannot transform interval expression "${interval}" to CubeStore dialect`);
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

  public hllCardinality(sql) {
    return `cardinality(${sql})`;
  }

  public castToString(sql) {
    return `CAST(${sql} as VARCHAR)`;
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
    const cumulativeMeasuresWithoutMultiplied = cumulativeMeasures.map(([_, measure]) => measure);
    const allMeasures = regularMeasures.concat(multipliedMeasures).concat(
      cumulativeMeasuresWithoutMultiplied
    );
    const timeDimension = this.timeDimensions.find(d => d.granularity || d.dateRange);
    const timeDimensionWithGranularity = timeDimension?.granularity ? timeDimension : null;
    const baseQueryAlias = this.cubeAlias('base');
    const maxRollingWindow = cumulativeMeasuresWithoutMultiplied.reduce((a, b) => this.maxRollingWindow(a, b.rollingWindowDefinition()), <RollingWindow><unknown>null);
    const commonDateCondition =
      this.rollingWindowDateJoinCondition(maxRollingWindow.trailing, maxRollingWindow.leading, maxRollingWindow.offset);
    const filters = this.segments.concat(this.filters).concat(
      timeDimension?.dateRange && this.dateFromStartToEndConditionSql(commonDateCondition, true, true) || []
    );
    const rollupGranularity = this.preAggregations?.castGranularity(preAggregationForQuery.preAggregation.granularity) || 'day';
    const granularityOverride = timeDimensionWithGranularity &&
      cumulativeMeasuresWithoutMultiplied.reduce((a, b) => this.minGranularity(a, b.windowGranularity()), timeDimensionWithGranularity.granularity) || rollupGranularity;
    return this.evaluateSymbolSqlWithContext(
      () => this.overTimeSeriesSelectRollup(
        cumulativeMeasuresWithoutMultiplied,
        regularMeasures.concat(multipliedMeasures),
        this.evaluateSymbolSqlWithContext(() => this.preAggregations?.rollupPreAggregation(preAggregationForQuery, allMeasures, false, filters), {
          granularityOverride,
          overTimeSeriesAggregate: true
        }),
        baseQueryAlias,
        timeDimensionWithGranularity,
        preAggregationForQuery
      ),
      {
        wrapQuery: true,
        wrappedGranularity: timeDimension?.granularity || rollupGranularity,
        rollupGranularity: granularityOverride,
        topLevelMerge: false,
        renderedReference: timeDimension ? {
          [timeDimension.dimension]: timeDimension.cumulativeSelectColumns()[0]
        } : undefined
      }
    );
  }

  public overTimeSeriesSelectRollup(cumulativeMeasures, otherMeasures, baseQuery, baseQueryAlias, timeDimension, preAggregationForQuery) {
    const cumulativeDimensions = this.dimensions.map(s => s.cumulativeSelectColumns()).filter(c => !!c).join(', ');
    const partitionByClause = this.dimensions.length ? `PARTITION BY ${cumulativeDimensions}` : '';
    const groupByDimensionClause = otherMeasures.length && timeDimension ? ` GROUP BY DIMENSION ${timeDimension.dimensionSql()}` : '';
    const rollingWindowOrGroupByClause = timeDimension ?
      ` ROLLING_WINDOW DIMENSION ${timeDimension.aliasName()}${partitionByClause}${groupByDimensionClause} FROM ${this.timeGroupedColumn(timeDimension.granularity, timeDimension.localDateTimeFromOrBuildRangeParam())} TO ${this.timeGroupedColumn(timeDimension.granularity, timeDimension.localDateTimeToOrBuildRangeParam())} EVERY INTERVAL '1 ${timeDimension.granularity}'` :
      this.groupByClause();
    const forSelect = this.overTimeSeriesForSelectRollup(cumulativeMeasures, otherMeasures, timeDimension, preAggregationForQuery);
    return `SELECT ${forSelect} FROM (${baseQuery}) ${baseQueryAlias}${rollingWindowOrGroupByClause}`;
  }

  public toInterval(interval) {
    if (interval === 'unbounded') {
      return 'UNBOUNDED';
    } else {
      return `INTERVAL '${interval}'`;
    }
  }

  public maxRollingWindow(a: RollingWindow, b: RollingWindow): RollingWindow {
    if (!a) {
      return b;
    }
    if (!b) {
      return a;
    }
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

    if ((a.offset || 'end') !== (b.offset || 'end')) {
      // TODO introduce virtual 'both' offset and return it if max receives 'start' and 'end'
      throw new Error('Mixed offset rolling window querying is not supported');
    }

    return {
      trailing,
      leading,
      offset: a.offset
    };
  }

  public overTimeSeriesForSelectRollup(cumulativeMeasures, otherMeasures, timeDimension, preAggregationForQuery) {
    const rollupMeasures = this.preAggregations?.rollupMeasures(preAggregationForQuery);
    const renderedReference = rollupMeasures.map(measure => {
      const m = this.newMeasure(measure);
      const renderSql = () => {
        if (timeDimension && m.isCumulative()) {
          const measureSql = m.cumulativeMeasureSql();
          const rollingWindow = m.rollingWindowDefinition();
          const preceding = rollingWindow.trailing ? `${this.toInterval(rollingWindow.trailing)} PRECEDING` : '';
          const following = rollingWindow.leading ? `${this.toInterval(rollingWindow.leading)} FOLLOWING` : '';
          const offset = ` OFFSET ${rollingWindow.offset || 'end'}`;
          const rollingMeasure = `ROLLING(${measureSql} ${preceding && following ? 'RANGE BETWEEN ' : 'RANGE '}${preceding}${preceding && following ? ' AND ' : ''}${following}${offset})`;
          return this.topAggregateWrap(m.measureDefinition(), rollingMeasure);
        } else {
          const conditionFn = m.isCumulative() ? this.dateFromStartToEndConditionSql(m.dateJoinCondition(), true, true)[0] : timeDimension;
          return this.evaluateSymbolSqlWithContext(
            () => {
              const aliasName = m.aliasName();
              return this.aggregateOnGroupedColumn(
                m.measureDefinition(),
                aliasName,
                true,
                m.measure
              );
            },
            {
              cumulativeMeasureFilters: { [m.measure]: conditionFn }
            }
          );
        }
      };

      return {
        [measure]: renderSql()
      };
    }).reduce((a, b) => ({ ...a, ...b }), {});
    return this.evaluateSymbolSqlWithContext(
      () => this.dimensions.concat(this.timeDimensions.filter(d => d.granularity)).map(s => s.cumulativeSelectColumns()).concat(
        this.measures.map(m => m.selectColumns())
      ).filter(c => !!c)
        .join(', '),
      {
        renderedReference
      }
    );
  }
}
