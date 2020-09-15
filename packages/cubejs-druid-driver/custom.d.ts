
declare module '@cubejs-backend/query-orchestrator/driver/BaseDriver' {
  abstract class BaseDriver {
    createSchemaIfNotExists(schemaName: string): Promise<any>;
    quoteIdentifier(identifier: string): string;
  }

  export default BaseDriver;
}
