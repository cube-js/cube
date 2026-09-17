import fs from 'fs';
import path from 'path';

import { downloadJDBCDriver, JDBC_DRIVER_JAR_NAME } from './installer';
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
    path.join(process.cwd(), JDBC_DRIVER_JAR_NAME),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', JDBC_DRIVER_JAR_NAME),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          `Please download and place ${JDBC_DRIVER_JAR_NAME} inside your ` +
          'project directory'
        );
      }
    )
  );
}

/**
 * Extract if exist UID and PWD from URL and return UID, PWD and URL without these params.
 * New Databricks OSS driver throws an error if any parameter is provided in the URL and as a separate param
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
    .replace(/;?PWD=[^;]*/i, '')
    .replace(/;?AuthMech=[^;]*/i, '');

  return [uid, pwd, cleanedUrl];
}

/**
 * Lift a parameter out of the JDBC URL so it can be forwarded through the properties map instead.
 * The OSS driver merges URL params and properties into a single map and throws
 * `IllegalArgumentException: Multiple entries with same key` when a (case-insensitive) key appears
 * in both, so anything we set as a property has to be removed from the URL first.
 */
export function extractAndRemoveUrlParam(jdbcUrl: string, param: string): [value: string | undefined, cleanedUrl: string] {
  const value = jdbcUrl.match(new RegExp(`${param}=([^;]*)`, 'i'))?.[1];

  return [value, jdbcUrl.replace(new RegExp(`;?${param}=[^;]*`, 'i'), '')];
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
