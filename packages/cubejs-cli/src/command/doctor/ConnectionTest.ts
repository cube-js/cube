import { BaseDriver } from '@cubejs-backend/query-orchestrator';

export class ConnectionTest {
  public createDriver = (config: any): Promise<BaseDriver> => {
    throw new Error('Unimplemented');
  };

  public handleErrors = (e: Error & { code: string }, config, _msg?: string): string => {
    throw new Error('Unimplemented');
  };
}
