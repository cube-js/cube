/* eslint-disable max-classes-per-file */
import R from 'ramda';

import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';

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
  public newFilter(filter) {
    return new ElasticSearchQueryFilter(this, filter);
  }

  public convertTz(field) {
    return `${field}`; // TODO
  }

  public timeStampCast(value) {
    return `${value}`;
  }

  public dateTimeCast(value) {
    return `${value}`; // TODO
  }

  public subtractInterval(date, interval) {
    // TODO: Test this, note sure how value gets populated here
    return `${date} - INTERVAL ${interval}`;
  }

  public addInterval(date, interval) {
    // TODO: Test this, note sure how value gets populated here
    return `${date} + INTERVAL ${interval}`;
  }

  public timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public unixTimestampSql() {
    return 'TIMESTAMP_DIFF(\'seconds\', \'1970-01-01T00:00:00.000Z\'::datetime, CURRENT_TIMESTAMP())';
  }

  public groupByClause() {
    if (this.ungrouped) {
      return '';
    }
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
      dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql())
    ).filter(s => !!s);

    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  public orderHashToString(hash) {
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

  public getFieldAlias(id) {
    const equalIgnoreCase = (a, b) => typeof a === 'string' &&
      typeof b === 'string' &&
      a.toUpperCase() === b.toUpperCase();

    let field;

    field = this.dimensionsForSelect().find(d => equalIgnoreCase(d.dimension, id));

    if (field) {
      return field.dimensionSql();
    }

    field = this.measures.find(
      d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id)
    );

    if (field) {
      return field.aliasName(); // TODO isn't supported
    }

    return null;
  }

  public escapeColumnName(name) {
    return `${name}`; // TODO
  }
}
