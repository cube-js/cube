export class BaseGroupFilter {
  protected readonly values: any;

  protected readonly operator: any;

  public readonly measure: any;

  public readonly dimension: any;

  public constructor(filter: any) {
    this.values = filter.values;
    this.operator = filter.operator;
    this.measure = filter.measure;
    this.dimension = filter.dimension;
  }

  public isDateOperator(): boolean {
    return false;
  }

  public conditionSql(column) {
    return `(\n${this.values.map(f => f.conditionSql(column)).join(` ${this.operator.toUpperCase()} `)}\n)`;
  }

  public filterToWhere() {
    const r = this.values.map(f => {
      const sql = f.filterToWhere();
      if (!sql) {
        return null;
      }
      return `(${sql})`;
    }).filter(x => x).join(` ${this.operator.toUpperCase()} `);

    if (!r.length) {
      return null;
    }
    return r;
  }

  public filterParams() {
    return this.values.map(f => f.filterParams());
  }

  public getMembers() {
    return this.values.map(f => {
      if (f.getMembers) {
        return f.getMembers();
      }
      return f;
    });
  }
}
