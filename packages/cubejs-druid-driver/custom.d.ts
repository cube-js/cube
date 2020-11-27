
declare module '@cubejs-backend/schema-compiler/adapter/BaseQuery' {
  abstract class BaseQuery {
    public escapeColumnName(identifier: string): string;
  }

  export default BaseQuery;
}
