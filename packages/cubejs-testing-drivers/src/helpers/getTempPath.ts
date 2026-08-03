import fs from 'fs-extra';
import path from 'path';

/**
 * Recreate the `./temp` and the `./.data` folders.
 */
export function getTempPath(): void {
  const temp = path.resolve(process.cwd(), './.temp');
  if (fs.pathExistsSync(temp)) {
    fs.rmSync(temp, { recursive: true });
  }

  fs.mkdirSync(temp);
  fs.mkdirSync(path.resolve(temp, './model'));
}
