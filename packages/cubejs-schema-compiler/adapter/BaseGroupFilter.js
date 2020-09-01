class BaseGroupFilter {
  constructor(query, filter) {
    this.values = filter.values;
    this.operator = filter.operator;
    this.measure = filter.measure;
    this.dimension = filter.dimension;
  }
      
  filterToWhere() {
    return this.values.map(f => `(${f.filterToWhere()})`).join(` ${this.operator} `);
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
  
module.exports = BaseGroupFilter;
