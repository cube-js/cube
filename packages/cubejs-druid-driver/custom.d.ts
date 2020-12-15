declare module '@cubejs-backend/schema-compiler/adapter/BaseQuery' {
  abstract class BaseQuery {
    protected readonly timezone: string;

    public escapeColumnName(identifier: string): string;
  }

  export default BaseQuery;
}
