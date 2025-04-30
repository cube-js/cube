import fs from 'fs';
import path from 'path';

import { downloadJDBCDriver, OSS_DRIVER_VERSION } from './installer';
import type { ParsedConnectionProperties } from './DatabricksDriver';

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
 * Extract if exist UID and PWD from URL and return UID, PWD and URL without these params.
 * New Databricks OSS driver throws an error if UID and PWD are provided in the URL and as a separate params
 * passed to the driver instance. That's why we strip them out from the URL if they exist there.
 * @param jdbcUrl
 */
export function extractAndRemoveUidPwdFromJdbcUrl(jdbcUrl: string): [uid: string, pwd: string, cleanedUrl: string] {
  const uidMatch = jdbcUrl.match(/UID=([^;]*)/i);
  const pwdMatch = jdbcUrl.match(/PWD=([^;]*)/i);

  const uid = uidMatch?.[1] || 'token';
  const pwd = pwdMatch?.[1] || '';

  const cleanedUrl = jdbcUrl
    .replace(/;?UID=[^;]*/i, '')
    .replace(/;?PWD=[^;]*/i, '');

  return [uid, pwd, cleanedUrl];
}

export function parseDatabricksJdbcUrl(jdbcUrl: string): ParsedConnectionProperties {
  const jdbcPrefix = 'jdbc:databricks://';
  const urlWithoutPrefix = jdbcUrl.slice(jdbcPrefix.length);

  const [hostPortAndPath, ...params] = urlWithoutPrefix.split(';');
  const [host] = hostPortAndPath.split(':');

  const paramMap = new Map<string, string>();
  for (const param of params) {
    const [key, value] = param.split('=');
    if (key && value) {
      paramMap.set(key, value);
    }
  }

  const httpPath = paramMap.get('httpPath');
  if (!httpPath) {
    throw new Error('Missing httpPath in JDBC URL');
  }

  const warehouseMatch = httpPath.match(/\/warehouses\/([a-zA-Z0-9]+)/);
  if (!warehouseMatch) {
    throw new Error('Could not extract warehouseId from httpPath');
  }

  const warehouseId = warehouseMatch[1];

  return { host, warehouseId };
}
