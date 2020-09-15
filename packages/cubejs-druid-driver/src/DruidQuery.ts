const BaseQuery = require('@cubejs-backend/schema-compiler/adapter/BaseQuery');

export class DruidQuery extends BaseQuery {
  timeStampParam() {
    return this.timeStampCast('?');
  }
}
