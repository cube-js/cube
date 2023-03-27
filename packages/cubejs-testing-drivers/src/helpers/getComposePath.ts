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
  const compose: any = {
    version: '2.2',
    services: {
      cube: {
        container_name: 'cube',
        image: 'cubejs/cube:testing-drivers',
        environment: cube.environment,
        volumes: cube.volumes,
        ports: cube.ports,
        restart: 'always',
      },
    },
  };
  if (data) {
    compose.services.data = {
      container_name: 'data',
      ...data,
    };
  }
  fs.writeFileSync(
    path.resolve(_path, _file),
    YAML.stringify(compose),
  );
  return [_path, _file];
}
