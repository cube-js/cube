import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { getEnv } from '@cubejs-backend/shared';
import { DatabricksQuery } from './DatabricksQuery';

interface DatabricksDriverConfiguration {

};

export class DatabricksDriver extends BaseDriver {
  public static dialectClass() {
    return DatabricksQuery;
  }

  public constructor(config?: DatabricksDriverConfiguration) {
    super();
  }

  public readOnly() {
    return true;
  }

  public async testConnection() {
    //
  }

  public async query(query: string, values: unknown[] = []): Promise<Array<unknown>> {
    return this.client.query(query, this.normalizeQueryValues(values));
  }

  public informationSchemaQuery() {
    return `
        SELECT
            COLUMN_NAME as ${this.quoteIdentifier('column_name')},
            TABLE_NAME as ${this.quoteIdentifier('table_name')},
            TABLE_SCHEMA as ${this.quoteIdentifier('table_schema')},
            DATA_TYPE as ${this.quoteIdentifier('data_type')}
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
    `;
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<unknown[]> {
    throw new Error('Unable to create schema, Druid does not support it');
  }

  public async getTablesQuery(schemaName: string) {
    return this.query('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?', [
      schemaName
    ]);
  }

  protected normalizeQueryValues(values: unknown[]) {
    return values.map((value) => ({
      value,
      type: 'VARCHAR',
    }));
  }

  protected normaliseResponse(res: any) {
    return res;
  }
}
