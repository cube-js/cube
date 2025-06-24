/* eslint-disable max-classes-per-file */
import R from 'ramda';

import { findMinGranularityDimension } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { BaseMeasure } from './BaseMeasure';
import { BaseDimension } from './BaseDimension';
import { BaseTimeDimension } from './BaseTimeDimension';

const GRANULARITY_TO_INTERVAL = {
  day: date => `DATE_TRUNC('day', ${date}::datetime)`,
  week: date => `DATE_TRUNC('week', ${date}::datetime)`,
  hour: date => `DATE_TRUNC('hour', ${date}::datetime)`,
  minute: date => `DATE_TRUNC('minute', ${date}::datetime)`,
  second: date => `DATE_TRUNC('second', ${date}::datetime)`,
  month: date => `DATE_TRUNC('month', ${date}::datetime)`,
  quarter: date => `DATE_TRUNC('quarter', ${date}::datetime)`,
  year: date => `DATE_TRUNC('year', ${date}::datetime)`
};

class ElasticSearchQueryFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    if (type === 'starts') {
      return `${not ? ' NOT' : ''} WHERE ${column} LIKE ${this.allocateParam(param)}%`;
    } else if (type === 'ends') {
      return `${not ? ' NOT' : ''} WHERE ${column} LIKE %${this.allocateParam(param)}`;
    } else {
      return `${not ? ' NOT' : ''} MATCH(${column}, ${this.allocateParam(param)}, 'fuzziness=AUTO:1,5')`;
    }
  }
}

export class ElasticSearchQuery extends BaseQuery {
  public override newFilter(filter) {
    return new ElasticSearchQueryFilter(this, filter);
  }

  public override convertTz(field) {
    return `${field}`; // TODO
  }

  public override timeStampCast(value) {
    return `${value}`;
  }

  public override dateTimeCast(value) {
    return `${value}`; // TODO
  }

  public override subtractInterval(date, interval) {
    // TODO: Test this, note sure how value gets populated here
    return `${date} - INTERVAL ${interval}`;
  }

  public override addInterval(date, interval) {
    // TODO: Test this, note sure how value gets populated here
    return `${date} + INTERVAL ${interval}`;
  }

  public override timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public override unixTimestampSql() {
    return 'TIMESTAMP_DIFF(\'seconds\', \'1970-01-01T00:00:00.000Z\'::datetime, CURRENT_TIMESTAMP())';
  }

  public override groupByClause() {
    if (this.ungrouped) {
      return '';
    }
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
      dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql())
    ).filter(s => !!s);

    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  public override orderHashToString(hash: { id: string, desc: boolean }) {
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

  /**
   * This implementation is a bit different from the one in BaseQuery
   * as it uses dimensionSql() as ordering expression
   */
  public getFieldAlias(id: string): string | null {
    const equalIgnoreCase = (a: any, b: any) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let field: BaseMeasure | BaseDimension | undefined;

    const path = id.split('.');

    // Granularity is specified
    if (path.length === 3) {
      const memberName = path.slice(0, 2).join('.');
      const granularity = path[2];

      field = this.timeDimensions
        // Not all time dimensions are used in select list, some are just filters,
        // but they exist in this.timeDimensions, so need to filter them out
        .filter(d => d.selectColumns())
        .find(
          d => (
            (equalIgnoreCase(d.dimension, memberName) && (d.granularityObj?.granularity === granularity)) ||
            equalIgnoreCase(d.expressionName, memberName)
          )
        );

      if (field) {
        return field.dimensionSql();
      }

      return null;
    }

    const dimensionsForSelect = this.dimensionsForSelect()
      // Not all time dimensions are used in select list, some are just filters,
      // but they exist in this.timeDimensions, so need to filter them out
      .filter(d => d.selectColumns());

    const found = findMinGranularityDimension(id, dimensionsForSelect);

    if (found?.dimension) {
      return (found.dimension as BaseDimension).dimensionSql();
    }

    field = this.measures.find(
      d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id)
    );

    if (field) {
      return field.aliasName(); // TODO isn't supported
    }

    return null;
  }

  public override escapeColumnName(name) {
    return `${name}`;
  }
}
