
declare module '@cubejs-backend/query-orchestrator/driver/BaseDriver' {
  class BaseDriver {
    quoteIdentifier(identifier: string): string;
  }

  export default BaseDriver;
}
