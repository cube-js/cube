import fs from 'fs-extra';
import path from 'path';

export function getCubeJsPath(type: string): string[] {
  const _path = path.resolve(process.cwd(), './.temp');
  const _file = '_cube.js';
  fs.copyFileSync(
    path.resolve(process.cwd(), `./fixtures/${_file}`),
    path.resolve(_path, 'cube.js'),
  );
  return [_path, 'cube.js'];
}
