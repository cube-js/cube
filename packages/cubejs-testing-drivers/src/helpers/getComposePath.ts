/* eslint-disable camelcase */
import fs from 'fs-extra';
import path from 'path';
import * as YAML from 'yaml';

import type { Fixture } from '../types/Fixture';

/**
 * Returns docker compose file by data source type.
 */
export function getComposePath(type: string, fixture: Fixture, isLocal: boolean): [path: string, file: string] {
  const _path = path.resolve(process.cwd(), './.temp');
  const _file = `${type}-compose.yaml`;

  const depends_on = ['store'];
  if (fixture.cube.depends_on) {
    depends_on.concat(fixture.cube.depends_on);
  }

  const links = ['store'];
  if (fixture.cube.links) {
    depends_on.concat(fixture.cube.links);
  }

  const volumes = [
    './cube.js:/cube/conf/cube.js',
    './package.json:/cube/conf/package.json',
    './model/ecommerce.yaml:/cube/conf/model/ecommerce.yaml',
  ];

  // Add AWS credential mounting for IRSA-enabled tests
  if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY && process.env.AWS_SESSION_TOKEN) {
    const awsCredentialsDir = path.resolve(_path, '.aws');
    fs.ensureDirSync(awsCredentialsDir);

    const credentialsContent = `[default]
aws_access_key_id = ${process.env.AWS_ACCESS_KEY_ID}
aws_secret_access_key = ${process.env.AWS_SECRET_ACCESS_KEY}
aws_session_token = ${process.env.AWS_SESSION_TOKEN}
`;

    const configContent = `[default]
region = ${process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-west-1'}
`;

    fs.writeFileSync(path.resolve(awsCredentialsDir, 'credentials'), credentialsContent);
    fs.writeFileSync(path.resolve(awsCredentialsDir, 'config'), configContent);

    volumes.push('./.aws:/root/.aws:ro');
  }
  const compose: any = {
    version: '2.2',
    services: {
      ...(!isLocal ? {
        cube: {
          ...fixture.cube,
          container_name: 'cube',
          image: 'cubejs/cube:testing-drivers',
          depends_on,
          links,
          volumes,
          restart: 'always',
        }
      } : {}),
      store: {
        container_name: 'store',
        image: `cubejs/cubestore:${process.arch === 'arm64' ? 'arm64v8' : 'latest'}`,
        ports: ['3030'],
        restart: 'always',
        ...(process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY && process.env.AWS_SESSION_TOKEN ? {
          volumes: ['./.aws:/root/.aws:ro']
        } : {})
      }
    }
  };

  if (fixture.data) {
    compose.services.data = {
      ...fixture.data,
      container_name: 'data',
    };
  }

  fs.writeFileSync(
    path.resolve(_path, _file),
    YAML.stringify(compose),
  );
  return [_path, _file];
}
