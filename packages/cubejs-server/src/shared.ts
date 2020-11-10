import path from 'path';
import color from '@oclif/color';
import * as fs from 'fs';

export const displayError = async (text: string|string[]) => {
  console.error('');
  console.error(color.cyan('Cube.js Error ---------------------------------------'));
  console.error('');

  if (Array.isArray(text)) {
    text.forEach((str) => console.error(str));
  } else {
    console.error(text);
  }

  console.error('');
  console.error(color.yellow('Need some help? -------------------------------------'));

  console.error('');
  console.error(`${color.yellow('  Ask this question in Cube.js Slack:')} https://slack.cube.dev`);
  console.error(`${color.yellow('  Post an issue:')} https://github.com/cube-js/cube.js/issues`);
  console.error('');

  process.exit(1);
};

export const packageExists = (
  moduleName: string,
  relative: boolean = false,
  basePath = process.cwd()
) => {
  if (relative) {
    try {
      // eslint-disable-next-line global-require,import/no-dynamic-require
      require.resolve(`${moduleName}`);

      return true;
    } catch (error) {
      return false;
    }
  }

  const modulePath = path.join(basePath, 'node_modules', moduleName);
  return fs.existsSync(modulePath);
};

type RequireFromPackageOptions = {
  basePath?: string,
  relative: boolean,
  silent?: true
}

export async function requireFromPackage<T = unknown>(
  pkg: string,
  { basePath = process.cwd(), relative, silent }: RequireFromPackageOptions
): Promise<T|null> {
  const exists = await packageExists(pkg, relative, basePath);
  if (!exists) {
    if (silent) {
      return null;
    }

    await displayError(
      `${pkg} dependency not found. Please run this command from project directory.`
    );
  }

  if (relative) {
    const resolvePath = require.resolve(`${pkg}`);

    // eslint-disable-next-line global-require,import/no-dynamic-require
    return require(resolvePath);
  }

  // eslint-disable-next-line global-require,import/no-dynamic-require
  return require(path.join(basePath, 'node_modules', pkg));
}
