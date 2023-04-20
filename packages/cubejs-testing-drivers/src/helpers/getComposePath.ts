/* eslint-disable camelcase */
import fs from 'fs-extra';
import path from 'path';
import * as YAML from 'yaml';
import { getFixtures } from './getFixtures';

/**
 * Returns docker compose file by data source type.
 */
export function getComposePath(type: string): [path: string, file: string] {
  const _path = path.resolve(process.cwd(), './.temp');
  const _file = `${type}-compose.yaml`;
  const { cube, data } = getFixtures(type);
  const depends_on = ['store'];
  if (cube.depends_on) {
    depends_on.concat(cube.depends_on);
  }
  const links = ['store'];
  if (cube.links) {
    depends_on.concat(cube.links);
  }
  const volumes = [
    './cube.js:/cube/conf/cube.js',
    './package.json:/cube/conf/package.json',
    './schema/ecommerce.yaml:/cube/conf/schema/ecommerce.yaml',
  ];
  const compose: any = {
    version: '2.2',
    services: {
      cube: {
        ...cube,
        container_name: 'cube',
        image: 'cubejs/cube:testing-drivers',
        depends_on,
        links,
        volumes,
        restart: 'always',
      },
      store: {
        container_name: 'store',
        image: 'cubejs/cubestore:latest',
        ports: ['3030'],
        restart: 'always',
      }
    },
  };
  if (data) {
    compose.services.data = {
      ...data,
      container_name: 'data',
    };
  }
  fs.writeFileSync(
    path.resolve(_path, _file),
    YAML.stringify(compose),
  );
  return [_path, _file];
}
