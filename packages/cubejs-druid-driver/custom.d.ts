
declare module '@cubejs-backend/query-orchestrator/driver/BaseDriver' {
  abstract class BaseDriver {
    public createSchemaIfNotExists(schemaName: string): Promise<any>;

    public quoteIdentifier(identifier: string): string;
  }

  export default BaseDriver;
}

declare module '@cubejs-backend/schema-compiler/adapter/BaseQuery' {
  abstract class BaseQuery {
    public escapeColumnName(identifier: string): string;
  }

  export default BaseQuery;
}
