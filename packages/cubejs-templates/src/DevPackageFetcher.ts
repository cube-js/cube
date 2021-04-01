import fs from 'fs-extra';
import path from 'path';
import PackageFetcher from './PackageFetcher';

export default class DevPackageFetcher extends PackageFetcher {
  init() {}

  async manifestJSON() {
    return JSON.parse(
      fs.readFileSync(path.join(this.tmpFolderPath, 'cubejs-playground-templates', 'manifest.json'), 'utf-8')
    );
  }

  async downloadRepo() {
  }

  async downloadPackages() {
    return {
      packagesPath: path.join(this.tmpFolderPath, 'cubejs-playground-templates', 'packages'),
    };
  }
}
