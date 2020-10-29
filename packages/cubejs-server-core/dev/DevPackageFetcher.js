/* eslint-disable */

const fs = require('fs-extra');
const path = require('path');

const PackageFetcher = require('./PackageFetcher');

class DevPackageFetcher extends PackageFetcher {
  init() {
  }
  
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

module.exports = DevPackageFetcher;
