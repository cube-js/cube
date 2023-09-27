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

export function extractUidFromJdbcUrl(url: string): string {
  const regex = /^jdbc:([^:]+):\/\/([^/]+)(\/[^;]+);(.+)$/;
  const match = url.match(regex);
  
  if (!match) {
    throw new Error(`Invalid JDBC URL: ${url}`);
  }

  const paramsString = match[4];
  const paramsArray = paramsString.split(';');
  const params: any = {};

  paramsArray.forEach(param => {
    const [key, value] = param.split('=');
    if (key && value) {
      params[key] = value;
    }
  });

  return params.UID || 'token';
}
