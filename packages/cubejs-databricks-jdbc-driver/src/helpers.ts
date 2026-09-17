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
 * Cube pins EnableGeoSpatialSupport off, so a URL asking to enable it can not be honoured and is
 * rejected rather than silently ignored. The driver enables the feature only on a literal `1`
 * (`"1".equals(value)`), so every other value already means off and is simply dropped — leaving it
 * in place would collide with the pinned property, which the driver rejects with
 * `IllegalArgumentException: Multiple entries with same key`.
 */
export function validateAndRemoveGeoSpatialSupportFromJdbcUrl(jdbcUrl: string): string {
  if (/;EnableGeoSpatialSupport=1(;|$)/i.test(jdbcUrl)) {
    throw new Error(
      'Unsupported configuration: EnableGeoSpatialSupport=1. Cube reads GEOMETRY/GEOGRAPHY columns ' +
      'as EWKT strings, please remove this parameter from the Databricks connection URL.'
    );
  }

  return jdbcUrl.replace(/;EnableGeoSpatialSupport=[^;]*/gi, '');
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
