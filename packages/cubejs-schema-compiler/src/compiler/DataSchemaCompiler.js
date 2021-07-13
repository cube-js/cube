import vm from 'vm';
import fs from 'fs';
import path from 'path';
import syntaxCheck from 'syntax-error';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';
import R from 'ramda';

import { UserError } from './UserError';
import { ErrorReporter } from './ErrorReporter';

const moduleFileCache = {};

export class DataSchemaCompiler {
  constructor(repository, options = {}) {
    this.repository = repository;
    this.cubeCompilers = options.cubeCompilers || [];
    this.contextCompilers = options.contextCompilers || [];
    this.transpilers = options.transpilers || [];
    this.preTranspileCubeCompilers = options.preTranspileCubeCompilers || [];
    this.cubeNameCompilers = options.cubeNameCompilers || [];
    this.extensions = options.extensions || {};
    this.cubeFactory = options.cubeFactory;
    this.filesToCompile = options.filesToCompile;
    this.omitErrors = options.omitErrors;
    this.allowNodeRequire = options.allowNodeRequire;
    this.compileContext = options.compileContext;
    this.compilerCache = options.compilerCache;
    this.errorReport = options.errorReport;
  }

  compileObjects(compileServices, objects, errorsReport) {
    try {
      return compileServices
        .map((compileService) => (() => compileService.compile(objects, errorsReport)))
        .reduce((p, fn) => p.then(fn), Promise.resolve())
        .catch((error) => {
          errorsReport.error(error);
        });
    } catch (e) {
      errorsReport.error(e);
      return Promise.resolve();
    }
  }

  compile() {
    if (!this.compilePromise) {
      this.compilePromise = this.repository.dataSchemaFiles().then((files) => {
        const toCompile = files.filter((f) => !this.filesToCompile || this.filesToCompile.indexOf(f.fileName) !== -1);

        const errorsReport = new ErrorReporter(null, [], this.errorReport);
        this.errorsReport = errorsReport;
        // TODO: required in order to get pre transpile compilation work
        const transpile = () => toCompile.map(f => this.transpileFile(f, errorsReport)).filter(f => !!f);

        const compilePhase = (compilers) => this.compileCubeFiles(compilers, transpile(), errorsReport);

        return compilePhase({ cubeCompilers: this.cubeNameCompilers })
          .then(() => compilePhase({ cubeCompilers: this.preTranspileCubeCompilers }))
          .then(() => compilePhase({
            cubeCompilers: this.cubeCompilers,
            contextCompilers: this.contextCompilers,
          }));
      }).then((res) => {
        if (!this.omitErrors) {
          this.throwIfAnyErrors();
        }
        return res;
      });
    }

    return this.compilePromise;
  }

  transpileFile(file, errorsReport) {
    try {
      const ast = parse(
        file.content,
        {
          sourceFilename: file.fileName,
          sourceType: 'module',
          plugins: ['objectRestSpread']
        },
      );

      this.transpilers.forEach((t) => {
        errorsReport.inFile(file);
        babelTraverse(ast, t.traverseObject(errorsReport));
        errorsReport.exitFile();
      });

      const content = babelGenerator(ast, {}, file.content).code;
      return Object.assign({}, file, { content });
    } catch (e) {
      if (e.toString().indexOf('SyntaxError') !== -1) {
        const line = file.content.split('\n')[e.loc.line - 1];
        const spaces = Array(e.loc.column).fill(' ').join('');
        errorsReport.error(`Syntax error during '${file.fileName}' parsing: ${e.message}:\n${line}\n${spaces}^`);
      } else {
        errorsReport.error(e);
      }
    }
    return undefined;
  }

  withQuery(query, fn) {
    const oldQuery = this.currentQuery;
    this.currentQuery = query;
    try {
      return fn();
    } finally {
      this.currentQuery = oldQuery;
    }
  }

  contextQuery() {
    return this.currentQuery;
  }

  async compileCubeFiles(compilers, toCompile, errorsReport) {
    const cubes = [];
    const exports = {};
    const contexts = [];
    const compiledFiles = {};
    const asyncModules = [];

    toCompile
      .forEach((file) => {
        this.compileFile(
          file,
          errorsReport,
          cubes,
          exports,
          contexts,
          toCompile,
          compiledFiles,
          asyncModules
        );
      });
    await asyncModules.reduce((a, b) => a.then(() => b()), Promise.resolve());
    return this.compileObjects(compilers.cubeCompilers || [], cubes, errorsReport)
      .then(() => this.compileObjects(compilers.contextCompilers || [], contexts, errorsReport));
  }

  throwIfAnyErrors() {
    this.errorsReport.throwIfAny();
  }

  compileFile(
    file, errorsReport, cubes, exports, contexts, toCompile, compiledFiles, asyncModules
  ) {
    if (compiledFiles[file.fileName]) {
      return;
    }

    compiledFiles[file.fileName] = true;
    const err = syntaxCheck(file.content, file.fileName);
    if (err) {
      errorsReport.error(err.toString());
    }
    try {
      vm.runInNewContext(file.content, {
        view: (name, cube) => cubes.push(Object.assign({}, cube, { name, fileName: file.fileName })),
        cube:
          (name, cube) => (
            !cube ?
              this.cubeFactory({ ...name, fileName: file.fileName }) :
              cubes.push(Object.assign({}, cube, { name, fileName: file.fileName }))
          ),
        context: (name, context) => contexts.push(Object.assign({}, context, { name, fileName: file.fileName })),
        addExport: (obj) => {
          exports[file.fileName] = exports[file.fileName] || {};
          exports[file.fileName] = Object.assign(exports[file.fileName], obj);
        },
        setExport: (obj) => {
          exports[file.fileName] = obj;
        },
        asyncModule: (fn) => {
          asyncModules.push(fn);
        },
        require: (extensionName) => {
          if (this.extensions[extensionName]) {
            return new (this.extensions[extensionName])(this.cubeFactory, this);
          } else {
            const foundFile = this.resolveModuleFile(file, extensionName, toCompile, errorsReport);
            if (!foundFile && this.allowNodeRequire) {
              if (extensionName.indexOf('.') === 0) {
                extensionName = path.resolve(this.repository.localPath(), extensionName);
              }
              // eslint-disable-next-line global-require,import/no-dynamic-require
              return require(extensionName);
            }
            this.compileFile(
              foundFile,
              errorsReport,
              cubes,
              exports,
              contexts,
              toCompile,
              compiledFiles
            );
            exports[foundFile.fileName] = exports[foundFile.fileName] || {};
            return exports[foundFile.fileName];
          }
        },
        COMPILE_CONTEXT: R.clone(this.compileContext || {})
      }, { filename: file.fileName, timeout: 15000 });
    } catch (e) {
      errorsReport.error(e);
    }
  }

  resolveModuleFile(currentFile, modulePath, toCompile, errorsReport) {
    const localImport = modulePath.match(/^\.\/(.*)$/);

    if (!currentFile.isModule && localImport) {
      const fileName = localImport[1].match(/^.*\.js$/) ? localImport[1] : `${localImport[1]}.js`;
      const foundFile = toCompile.find((f) => f.fileName === fileName);
      if (!foundFile) {
        throw new UserError(`Required import for ${fileName} is not found`);
      }
      return foundFile;
    }

    const nodeModulesPath = path.resolve('node_modules');
    let absPath = currentFile.isModule ?
      path.resolve('node_modules', path.dirname(currentFile.fileName), modulePath) :
      path.resolve('node_modules', modulePath);

    if (absPath.indexOf(nodeModulesPath) !== 0) {
      if (this.allowNodeRequire) {
        return null;
      }
      throw new UserError(`'${modulePath}' restricted`);
    }
    const packagePath = absPath.replace(nodeModulesPath, '').split('/').filter(s => !!s)[0];
    if (!packagePath) {
      if (this.allowNodeRequire) {
        return null;
      }
      throw new UserError(`'${modulePath}' is incorrect`);
    }
    if (!this.isWhiteListedPackage(packagePath)) {
      if (this.allowNodeRequire) {
        return null;
      }
      throw new UserError(`Package '${packagePath}' not found`);
    }
    if (fs.existsSync(absPath)) {
      const stat = fs.lstatSync(absPath);
      if (stat.isDirectory()) {
        absPath = path.resolve(absPath, 'index.js');
      }
    }
    // eslint-disable-next-line prefer-template
    absPath = path.extname(absPath) !== '.js' ? absPath + '.js' : absPath;
    if (!fs.existsSync(absPath)) {
      if (this.allowNodeRequire) {
        return null;
      }
      // eslint-disable-next-line prefer-template
      throw new UserError(`Path '${absPath.replace(nodeModulesPath + '/', '')}' not found`);
    }
    return this.readModuleFile(absPath, errorsReport);
  }

  readModuleFile(absPath, errorsReport) {
    const nodeModulesPath = path.resolve('node_modules');
    if (!moduleFileCache[absPath]) {
      const content = fs.readFileSync(absPath, 'utf-8');
      // eslint-disable-next-line prefer-template
      const fileName = absPath.replace(nodeModulesPath + '/', '');
      const transpiled = this.transpileFile(
        { fileName, content, isModule: true },
        errorsReport
      );
      if (!transpiled) {
        throw new UserError(`'${fileName}' transpiling failed`);
      }
      moduleFileCache[absPath] = transpiled; // TODO isolated transpiling
    }
    return moduleFileCache[absPath];
  }

  isWhiteListedPackage(packagePath) {
    return packagePath.indexOf('-schema') !== -1 &&
      (packagePath.indexOf('-schema') === packagePath.length - '-schema'.length);
  }
}
