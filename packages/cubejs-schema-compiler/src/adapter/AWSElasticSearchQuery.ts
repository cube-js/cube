import R from 'ramda';
import { BaseFilter } from './BaseFilter';
import { BaseQuery } from './BaseQuery';

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd 00:00:00.000')`,
  // eslint-disable-next-line no-unused-vars,@typescript-eslint/no-unused-vars
  week: (date) => { throw new Error('Week is unsupported'); }, // TODO
  hour: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:00:00.000')`,
  minute: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:00.000')`,
  second: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss.000')`,
  month: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-01 00:00:00.000')`,
  year: (date) => `DATE_FORMAT(${date}, 'yyyy-01-01 00:00:00.000')`
};

class AWSElasticSearchQueryFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class AWSElasticSearchQuery extends BaseQuery {
  public newFilter(filter) {
    return new AWSElasticSearchQueryFilter(this, filter);
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
    return `DATE_SUB(${date}, INTERVAL ${interval})`;
  }

  public addInterval(date, interval) {
    return `DATE_ADD(${date}, INTERVAL ${interval})`;
  }

  public timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public groupByClause() {
    if (this.ungrouped) {
      return '';
    }
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql()))
      .filter(s => !!s);
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
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let field;

    field = this.dimensionsForSelect().find(d => equalIgnoreCase(d.dimension, id));

    if (field) {
      return field.dimensionSql();
    }

    field = this.measures.find(d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id));

    if (field) {
      return field.aliasName(); // TODO isn't supported
    }

    return null;
  }

  public escapeColumnName(name) {
    return `${name}`; // TODO
  }
}
