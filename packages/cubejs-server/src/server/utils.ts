import path from 'path';
import fs from 'fs';

export const packageExists = (moduleName: string) => {
  const modulePath = path.join(process.cwd(), 'node_modules', moduleName);
  return fs.existsSync(modulePath);
};
