import fs from 'fs-extra';
import path from 'path';
import { PackageFetcher } from './PackageFetcher';

export class DevPackageFetcher extends PackageFetcher {
  protected init() {
    //
  }

  public async manifestJSON() {
    return JSON.parse(
      fs.readFileSync(path.join(this.tmpFolderPath, 'cubejs-playground-templates', 'manifest.json'), 'utf-8')
    );
  }

  public async downloadRepo() {
    //
  }

  public async downloadPackages() {
    return {
      packagesPath: path.join(this.tmpFolderPath, 'cubejs-playground-templates', 'packages'),
    };
  }
}
