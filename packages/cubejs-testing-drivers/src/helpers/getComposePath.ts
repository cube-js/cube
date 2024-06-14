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
