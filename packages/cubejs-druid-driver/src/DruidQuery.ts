const BaseQuery = require('@cubejs-backend/schema-compiler/adapter/BaseQuery');

export class DruidQuery extends BaseQuery {
  // eslint-disable-next-line no-unused-vars
  timeStampParam(timeDimension: string) {
    return this.timeStampCast(`?`);
  }
}
