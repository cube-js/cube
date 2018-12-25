class ParamAllocator {
  constructor() {
    this.params = [];
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
    return [annotatedSql.replace(/\$(\d+)\$/g, (match, paramIndex) => {
      paramsInSqlOrder.push(this.params[paramIndex]);
      return this.paramPlaceHolder(paramsInSqlOrder.length - 1);
    }), paramsInSqlOrder];
  }

  paramPlaceHolder(paramIndex) {
    return '?';
  }
}

module.exports = ParamAllocator;