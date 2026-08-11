import fs from 'fs-extra';
import * as tar from 'tar';
import path from 'path';
import { executeCommand } from '@cubejs-backend/shared';

import { proxyFetch } from './utils';

type Repository = {
  name: string;
  owner: string;
};

export class PackageFetcher {
  protected readonly tmpFolderPath: string;

  protected repoArchivePath: string;

  public constructor(private readonly repo: Repository) {
    this.tmpFolderPath = path.resolve('.', 'node_modules', '.tmp');

    this.init();

    this.repoArchivePath = `${this.tmpFolderPath}/master.tar.gz`;
  }

  protected init() {
    try {
      // Folder node_modules does not exist by default inside docker in /cube/conf without sharing volume for it
      fs.mkdirpSync(this.tmpFolderPath);
    } catch (err: any) {
      if (err.code === 'EEXIST') {
        this.cleanup();
        fs.mkdirSync(this.tmpFolderPath);
      } else {
        throw err;
      }
    }
  }

  public async manifestJSON() {
    const response = await proxyFetch(
      `https://api.github.com/repos/${this.repo.owner}/${this.repo.name}/contents/manifest.json`
    );

    return JSON.parse(Buffer.from((await response.json()).content, 'base64').toString());
  }

  protected async downloadRepo() {
    const url = `https://github.com/${this.repo.owner}/${this.repo.name}/archive/master.tar.gz`;
    const writer = fs.createWriteStream(this.repoArchivePath);

    (await proxyFetch(url)).body.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', resolve as () => void);
      writer.on('error', reject);
    });
  }

  public async downloadPackages() {
    await this.downloadRepo();

    // `decompress` + `decompress-targz` are unmaintained and carry two unfixed
    // archive-extraction advisories (GHSA-mp2f-45pm-3cg9, GHSA-h39j-r5qq-r9mm).
    // This call site only ever handled gzipped tars — it passed the targz plugin
    // explicitly — so `tar.x` is a direct equivalent, and it refuses to write
    // outside `cwd`: leading `/` is stripped and entries containing `..` are
    // dropped.
    await tar.x({ file: this.repoArchivePath, cwd: this.tmpFolderPath });

    const dir = fs.readdirSync(this.tmpFolderPath).find((name) => !name.endsWith('tar.gz'));

    if (!dir) {
      throw new Error('No directory found');
    }

    fs.removeSync(path.resolve(this.tmpFolderPath, dir, 'yarn.lock'));
    await executeCommand('npm', ['install'], { cwd: path.resolve(this.tmpFolderPath, dir) });

    return {
      packagesPath: path.join(this.tmpFolderPath, dir, 'packages'),
    };
  }

  public cleanup() {
    fs.removeSync(this.tmpFolderPath);
  }
}
