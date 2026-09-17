import fs from 'fs';
import path from 'path';

import { parseJdbcUrl } from '@cubejs-backend/jdbc-driver';

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
 * The OSS driver throws if a parameter is passed both in the URL and as a separate property, so the
 * credentials are stripped from the URL and handed over as properties instead.
 */
export function extractAndRemoveUidPwdFromJdbcUrl(jdbcUrl: string): [uid: string, pwd: string, cleanedUrl: string] {
  const parsed = parseJdbcUrl(jdbcUrl);

  const uid = parsed.get('UID')?.value || 'token';
  const pwd = parsed.get('PWD')?.value || '';

  return [uid, pwd, parsed.without(['UID', 'PWD', 'AuthMech']).toString()];
}

export function validateAndRemoveGeoSpatialSupportFromJdbcUrl(jdbcUrl: string): string {
  const parsed = parseJdbcUrl(jdbcUrl);

  if (parsed.getAll('EnableGeoSpatialSupport').some(({ value }) => value === '1')) {
    throw new Error(
      'Unsupported configuration: EnableGeoSpatialSupport=1. Cube reads GEOMETRY/GEOGRAPHY columns ' +
      'as EWKT strings, please remove this parameter from the Databricks connection URL.'
    );
  }

  return parsed.without(['EnableGeoSpatialSupport']).toString();
}

export function parseDatabricksJdbcUrl(jdbcUrl: string): ParsedConnectionProperties {
  const parsed = parseJdbcUrl(jdbcUrl);
  const [host] = parsed.base.slice('jdbc:databricks://'.length).split(':');

  const httpPath = parsed.get('httpPath')?.value;
  if (!httpPath) {
    throw new Error('Missing httpPath in JDBC URL');
  }

  const warehouseMatch = httpPath.match(/\/warehouses\/([a-zA-Z0-9]+)/);
  if (!warehouseMatch) {
    throw new Error('Could not extract warehouseId from httpPath');
  }

  return { host, warehouseId: warehouseMatch[1] };
}
