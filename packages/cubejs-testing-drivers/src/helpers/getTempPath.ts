import fs from 'fs-extra';
import path from 'path';

/**
 * Recreate the `./temp` and the `./.data` folders.
 */
export function getTempPath(): void {
  const _temp = path.resolve(process.cwd(), './.temp');
  if (fs.pathExistsSync(_temp)) {
    fs.rmdirSync(_temp, { recursive: true });
  }
  fs.mkdirSync(_temp);
  fs.mkdirSync(path.resolve(_temp, './schema'));
}
