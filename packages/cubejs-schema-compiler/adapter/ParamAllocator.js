class ParamAllocator {
  constructor() {
    this.params = [];
    this.paramsMatchRegex = /\$(\d+)\$/g;
  }

  allocateParam(param) {
    const paramIndex = this.params.length;
    this.params.push(param);
    return `$${paramIndex}$`;
  }

  allocateParamsForQuestionString(sql, paramArray) {
    let paramIndex = 0;
    return sql.replace(/\?/g, () => this.allocateParam(paramArray[paramIndex++]));
  }

  buildSqlAndParams(annotatedSql) {
    const paramsInSqlOrder = [];
    return [annotatedSql.replace(this.paramsMatchRegex, (match, paramIndex) => {
      paramsInSqlOrder.push(this.params[paramIndex]);
      return this.paramPlaceHolder(paramsInSqlOrder.length - 1);
    }), paramsInSqlOrder];
  }

  // eslint-disable-next-line no-unused-vars
  paramPlaceHolder(paramIndex) {
    return '?';
  }
}

module.exports = ParamAllocator;
