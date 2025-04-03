import fs from 'fs';
import path from 'path';

import { downloadJDBCDriver } from './installer';

async function fileExistsOr(
  fsPath: string,
  fn: () => Promise<string>,
): Promise<string> {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }
  return fn();
}

export async function resolveJDBCDriver(): Promise<string> {
  return fileExistsOr(
    path.join(process.cwd(), 'DatabricksJDBC42.jar'),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', 'DatabricksJDBC42.jar'),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          'Please download and place DatabricksJDBC42.jar inside your ' +
          'project directory'
        );
      }
    )
  );
}

export function extractUidFromJdbcUrl(jdbcUrl: string): string {
  const { pathname } = new URL(jdbcUrl);
  const [_, ...params] = pathname.split(';');
  const searchParams = new URLSearchParams(params.join('&'));
  return searchParams.get('UID') || 'token';
}
