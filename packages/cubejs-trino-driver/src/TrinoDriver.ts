import fetch from 'node-fetch';
import { PrestoDriver } from '@cubejs-backend/prestodb-driver';
import { PrestodbQuery } from '@cubejs-backend/schema-compiler';

export class TrinoDriver extends PrestoDriver {
  public constructor(options: any) {
    super({ ...options, engine: 'trino' });
  }

  public static dialectClass() {
    return PrestodbQuery;
  }

  // eslint-disable-next-line consistent-return
  public override async testConnection(): Promise<void> {
    if (this.useSelectTestConnection) {
      return this.testConnectionViaSelect();
    }

    const { host, port, ssl, basic_auth: basicAuth, custom_auth: customAuth } = this.config;
    const protocol = ssl ? 'https' : 'http';
    const url = `${protocol}://${host}:${port}/v1/info`;
    const headers: Record<string, string> = {};

    if (customAuth) {
      headers.Authorization = customAuth;
    } else if (basicAuth) {
      const { user, password } = basicAuth;
      const encoded = Buffer.from(`${user}:${password}`).toString('base64');
      headers.Authorization = `Basic ${encoded}`;
    }

    const response = await fetch(url, { method: 'GET', headers });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Connection test failed: ${response.status} ${response.statusText} - ${text}`);
    }
  }
}
