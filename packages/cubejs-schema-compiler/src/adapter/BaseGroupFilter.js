import R from 'ramda';

export class BaseGroupFilter {
  constructor(query, filter) {
    this.values = filter.values;
    this.operator = filter.operator;
    this.measure = filter.measure;
    this.dimension = filter.dimension;
  }

  filterToWhere() {
    const r = this.values.map(f => {
      const sql = f.filterToWhere();
      if (!sql) {
        return null;
      }
      return `(${sql})`;
    }).filter(R.identity).join(` ${this.operator} `);

    if (!r.length) {
      return null;
    }
    return r;
  }

  getMembers() {
    return this.values.map(f => {
      if (f.getMembers) {
        return f.getMembers();
      }
      return f;
    });
  }
}
