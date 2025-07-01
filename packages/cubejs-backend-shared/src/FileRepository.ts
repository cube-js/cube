import path from 'path';
import fs from 'fs-extra';

export interface FileContent {
  fileName: string;
  content: string;
  readOnly?: boolean;
  isModule?: boolean;
}

export interface SchemaFileRepository {
  localPath: () => string;
  dataSchemaFiles: (includeDependencies?: boolean) => Promise<FileContent[]>;
}

export class FileRepository implements SchemaFileRepository {
  public constructor(
    protected readonly repositoryPath: string
  ) {
  }

  public localPath(): string {
    return path.join(process.cwd(), this.repositoryPath);
  }

  protected async getFiles(dir: string, fileList: string[] = []): Promise<string[]> {
    let files: string[] = [];

    try {
      const fullPath = path.join(this.localPath(), dir);
      await fs.ensureDir(fullPath);
      files = await fs.readdir(fullPath);
    } catch (e) {
      throw new Error(`Model files not found. Please make sure the "${this.repositoryPath}" directory exists and contains model files.`);
    }

    // eslint-disable-next-line no-restricted-syntax
    for (const file of files) {
      const stat = await fs.stat(path.join(this.localPath(), dir, file));
      if (stat.isDirectory()) {
        fileList = await this.getFiles(path.join(dir, file), fileList);
      } else fileList.push(path.join(dir, file));
    }

    return fileList;
  }

  public async dataSchemaFiles(includeDependencies: boolean = false): Promise<FileContent[]> {
    const files = await this.getFiles('');

    let result = await Promise.all(
      files
        .filter(file => file.endsWith('.js') || file.endsWith('.yml') || file.endsWith('.yaml') || file.endsWith('.jinja') || file.endsWith('.py'))
        .map(async file => {
          const content = await fs.readFile(path.join(this.localPath(), file), 'utf-8');

          return {
            fileName: file,
            content
          };
        })
    );

    if (includeDependencies) {
      result = result.concat(await this.readModules());
    }

    return result;
  }

  public writeDataSchemaFile(fileName: string, source: string) {
    fs.writeFileSync(path.join(this.localPath(), fileName), source, {
      encoding: 'utf-8'
    });
  }

  protected async readModules() {
    const packageJson = JSON.parse(await fs.readFile('package.json', 'utf-8'));

    const files = await Promise.all(
      Object.keys(packageJson.dependencies).map(async module => {
        if (module.endsWith('-schema')) {
          return this.readModuleFiles(path.join('node_modules', module));
        }

        return [];
      })
    );

    return files.reduce((a, b) => a.concat(b));
  }

  protected async readModuleFiles(modulePath: string): Promise<FileContent[]> {
    const files = await fs.readdir(modulePath);

    const result = await Promise.all(
      files.map(async file => {
        const fileName = path.join(modulePath, file);
        const stats = await fs.lstat(fileName);
        if (stats.isDirectory()) {
          return this.readModuleFiles(fileName);
        } else if (file.endsWith('.js')) {
          const content = await fs.readFile(fileName, 'utf-8');
          return [
            {
              fileName,
              content,
              readOnly: true
            }
          ];
        } else {
          return [];
        }
      })
    );

    return result.reduce<FileContent[]>((a, b) => a.concat(b), []);
  }
}
