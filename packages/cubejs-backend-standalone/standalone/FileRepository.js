const path = require('path');
const promisifyAll = require('es6-promisify-all');
const fs = promisifyAll(require('fs'));
const R = require('ramda');

class FileRepository {
  constructor(repositoryPath) {
    this.repositoryPath = repositoryPath;
  }

  localPath() {
    return path.join(process.cwd(), this.repositoryPath);
  }

  async dataSchemaFiles(includeDependencies) {
    const self = this;
    const files = await fs.readdirAsync(this.localPath());
    let result = await Promise.all(
      files.filter((file) => R.endsWith('.js', file))
        .map(async (file) => {
          const content = await fs.readFileAsync(path.join(self.localPath(), file), "utf-8");
          return { fileName: file, content };
        })
    );
    if (includeDependencies) {
      result = result.concat(await this.readModules());
    }
    return result;
  }

  async readModules() {
    const packageJson = JSON.parse(await fs.readFileAsync('package.json', 'utf-8'));
    const files = await Promise.all(Object.keys(packageJson.dependencies).map(async module => {
      if (R.endsWith('-schema', module)) {
        return this.readModuleFiles(path.join('node_modules', module));
      }
      return [];
    }));
    return files.reduce((a, b) => a.concat(b));
  }

  async readModuleFiles(modulePath) {
    const files = await fs.readdirAsync(modulePath);
    return (await Promise.all(files
      .map(async file => {
        const fileName = path.join(modulePath, file);
        const stats = await fs.lstatAsync(fileName);
        if (stats.isDirectory()) {
          return this.readModuleFiles(fileName);
        } else if (R.endsWith('.js', file)) {
          const content = await fs.readFileAsync(fileName, "utf-8");
          return [{
            fileName, content, readOnly: true
          }]
        } else {
          return [];
        }
      })
    )).reduce((a, b) => a.concat(b), [])
  }
}

module.exports = FileRepository;