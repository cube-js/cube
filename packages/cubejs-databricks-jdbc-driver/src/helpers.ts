import fs from 'fs';
import path from 'path';

import { downloadJDBCDriver, OSS_DRIVER_VERSION } from './installer';

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
    path.join(process.cwd(), `databricks-jdbc-${OSS_DRIVER_VERSION}-oss.jar`),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', `databricks-jdbc-${OSS_DRIVER_VERSION}-oss.jar`),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          `Please download and place databricks-jdbc-${OSS_DRIVER_VERSION}-oss.jar inside your ` +
          'project directory'
        );
      }
    )
  );
}

/**
 * Extract if exist UID and PWD from URL and return UID, PWD and URL without these params
 * @param jdbcUrl
 */
export function extractAndRemoveUidPwdFromJdbcUrl(jdbcUrl: string): [string, string, string] {
  const uidMatch = jdbcUrl.match(/UID=([^;]*)/);
  const pwdMatch = jdbcUrl.match(/PWD=([^;]*)/);

  const uid = uidMatch?.[1] || 'token';
  const pwd = pwdMatch?.[1] || '';

  const cleanedUrl = jdbcUrl
    .replace(/;?UID=[^;]*/i, '')
    .replace(/;?PWD=[^;]*/i, '');

  return [uid, pwd, cleanedUrl];
}
