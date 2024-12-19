import { createWriteStream } from 'fs';
import { getHttpAgentForProxySettings } from '@cubejs-backend/shared';
import { pipeline } from 'node:stream/promises';
import fetch from 'node-fetch';

const URL = 'https://repo1.maven.org/maven2/org/mongodb/mongodb-jdbc/2.2.0/mongodb-jdbc-2.2.0-all.jar';

export async function downloadJDBCDriver(path: string) {
  const response = await fetch(URL, {
    agent: await getHttpAgentForProxySettings(),
  });

  if (!response.ok) {
    throw new Error(`Failed to download MongoDB JDBC driver (status=${response.status})`);
  }

  const writeStream = createWriteStream(path);

  await pipeline(response.body, writeStream);
}
