import { execFileSync } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';
import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { StartedTestContainer } from 'testcontainers';
import { MssqlDbRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  stopIfStarted,
} from './smoke-tests';

/**
 * Creates a private CA and a server certificate for `localhost` signed by it,
 * plus an mssql.conf that forces encrypted connections with that certificate.
 */
function prepareTlsFixtures(): { dir: string, caPem: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cube-mssql-ssl-'));
  const openssl = (...args: string[]) => execFileSync('openssl', args, { cwd: dir, stdio: 'pipe' });

  openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2',
    '-subj', '/CN=Cube Test CA', '-keyout', 'ca.key', '-out', 'ca.pem');
  openssl('req', '-newkey', 'rsa:2048', '-nodes',
    '-subj', '/CN=localhost', '-keyout', 'server.key', '-out', 'server.csr');
  fs.writeFileSync(
    path.join(dir, 'server.ext'),
    'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n'
  );
  openssl('x509', '-req', '-in', 'server.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key',
    '-CAcreateserial', '-days', '2', '-extfile', 'server.ext', '-out', 'server.pem');
  fs.writeFileSync(
    path.join(dir, 'mssql.conf'),
    '[network]\ntlscert = /certs/server.pem\ntlskey = /certs/server.key\ntlsprotocols = 1.2\nforceencryption = 1\n'
  );

  // SQL Server runs as a non-root user inside the container.
  fs.chmodSync(dir, 0o755);

  for (const file of fs.readdirSync(dir)) {
    fs.chmodSync(path.join(dir, file), 0o644);
  }

  return { dir, caPem: fs.readFileSync(path.join(dir, 'ca.pem'), 'utf8') };
}

// https://github.com/cube-js/cube/issues/8244
// The SSL recipe linked from the SQL Server data source docs says that a custom
// CA can be provided with CUBEJS_DB_SSL_CA, but the MSSQL driver only passes
// `encrypt` to the driver and ignores the CA, so the TLS handshake fails with
// "unable to verify the first certificate".
describe('mssql with SSL and a custom CA', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;
  let fixturesDir: string;

  beforeAll(async () => {
    const tls = prepareTlsFixtures();
    fixturesDir = tls.dir;

    db = await MssqlDbRunner.startContainer({
      volumes: [
        { source: tls.dir, target: '/certs', bindMode: 'ro' },
        { source: path.join(tls.dir, 'mssql.conf'), target: '/var/opt/mssql/mssql.conf', bindMode: 'ro' },
      ],
    });

    birdbox = await getBirdbox(
      'mssql',
      {
        CUBEJS_DB_TYPE: 'mssql',

        // Must match the certificate's subject alternative names.
        CUBEJS_DB_HOST: 'localhost',
        CUBEJS_DB_PORT: `${db.getMappedPort(1433)}`,
        CUBEJS_DB_USER: 'sa',
        CUBEJS_DB_PASS: process.env.TEST_DB_PASSWORD || 'Test1test',
        CUBEJS_DB_SSL: 'true',
        CUBEJS_DB_SSL_CA: tls.caPem,

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'mssql/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await stopIfStarted('birdbox', birdbox);
    await stopIfStarted('mssql', db);
    if (fixturesDir) {
      fs.rmSync(fixturesDir, { recursive: true, force: true });
    }
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('connects over TLS using CUBEJS_DB_SSL_CA', async () => {
    const response = await client.load({
      measures: [
        'Orders.totalAmount',
      ],
    });

    expect(response.rawData()).toEqual([
      { 'Orders.totalAmount': '2400' },
    ]);
  });
});
