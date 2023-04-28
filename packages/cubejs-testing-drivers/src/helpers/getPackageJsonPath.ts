import fs from 'fs-extra';
import path from 'path';

export function getPackageJsonPath(type: string): string[] {
  const _path = path.resolve(process.cwd(), './.temp');
  const _file = '_package.json';
  fs.copyFileSync(
    path.resolve(process.cwd(), `./fixtures/${_file}`),
    path.resolve(_path, 'package.json'),
  );
  return [_path, 'package.json'];
}
