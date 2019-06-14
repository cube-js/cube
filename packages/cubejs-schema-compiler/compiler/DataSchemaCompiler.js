const vm = require('vm');
const syntaxCheck = require('syntax-error');
const CompileError = require('./CompileError');
const UserError = require('./UserError');
const babylon = require("babylon");
const babelGenerator = require("babel-generator").default;
const babelTraverse = require("babel-traverse").default;
const fs = require('fs');
const path = require('path');

const moduleFileCache = {};

class ErrorReporter {
  constructor(parent, context) {
    this.errors = [];
    this.parent = parent;
    this.context = context || [];
  }

  error(e, fileName, lineNumber, position) {
    const message = `${this.context.length ? `${this.context.join(' -> ')}: ` : ''}${e instanceof UserError ? e.message : (e.stack || e)}`;
    if (this.rootReporter().errors.find(m => (m.message || m) === message)) {
      return;
    }
    if (fileName) {
      this.rootReporter().errors.push({ message, fileName, lineNumber, position });
    } else {
      this.rootReporter().errors.push(message);
    }
  }

  rootReporter() {
    return this.parent ? this.parent.rootReporter() : this;
  }

  inContext(context) {
    return new ErrorReporter(this, this.context.concat(context));
  }

  throwIfAny() {
    if (this.rootReporter().errors.length > 0) {
      throw new CompileError(this.rootReporter().errors);
    }
  }
}

class DataSchemaCompiler {
  constructor(repository, options) {
    options = options || {};
    this.repository = repository;
    this.cubeCompilers = options.cubeCompilers || [];
    this.contextCompilers = options.contextCompilers || [];
    this.dashboardTemplateCompilers = options.dashboardTemplateCompilers || [];
    this.transpilers = options.transpilers || [];
    this.preTranspileCubeCompilers = options.preTranspileCubeCompilers || [];
    this.cubeNameCompilers = options.cubeNameCompilers || [];
    this.extensions = options.extensions || {};
    this.cubeFactory = options.cubeFactory;
    this.filesToCompile = options.filesToCompile;
    this.omitErrors = options.omitErrors;
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
    const self = this;
    return this.repository.dataSchemaFiles().then((files) => {
      const toCompile = files.filter((f) => !this.filesToCompile || this.filesToCompile.indexOf(f.fileName) !== -1);

      const errorsReport = new ErrorReporter();
      this.errorsReport = errorsReport;
      // TODO: required in order to get pre transpile compilation work
      const transpile = () => toCompile.map(f => this.transpileFile(f, errorsReport)).filter(f => !!f);

      const compilePhase = (compilers) =>
        self.compileCubeFiles(compilers, transpile(), errorsReport);

      return compilePhase({ cubeCompilers: this.cubeNameCompilers })
        .then(() => compilePhase({ cubeCompilers: this.preTranspileCubeCompilers }))
        .then(() => compilePhase({
          cubeCompilers: this.cubeCompilers,
          contextCompilers: this.contextCompilers,
          dashboardTemplateCompilers: this.dashboardTemplateCompilers
        }));
    }).then((res) => {
      if (!this.omitErrors) {
        this.throwIfAnyErrors();
      }
      return res;
    });
  }

  transpileFile(file, errorsReport) {
    try {
      babelTraverse.clearCache();
      const ast = babylon.parse(file.content, { sourceFilename: file.fileName, sourceType: 'module' });
      this.transpilers.forEach((t) => babelTraverse(ast, t.traverseObject()));
      const content = babelGenerator(ast, {}, file.content).code;
      return Object.assign({}, file, { content });
    } catch (e) {
      if (e.toString().indexOf('SyntaxError') !== -1) {
        errorsReport.error(`Syntax error during '${file.fileName}' parsing: ${e.message}`);
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

  compileCubeFiles(compilers, toCompile, errorsReport) {
    const self = this;
    const cubes = [];
    const exports = {};
    const contexts = [];
    const dashboardTemplates = [];
    const compiledFiles = {};
    toCompile
      .forEach((file) => {
        this.compileFile(file, errorsReport, cubes, exports, contexts, dashboardTemplates, toCompile, compiledFiles);
      });
    return self.compileObjects(compilers.cubeCompilers || [], cubes, errorsReport)
           .then(() => self.compileObjects(compilers.contextCompilers || [], contexts, errorsReport))
           .then(() => self.compileObjects(compilers.dashboardTemplateCompilers || [], dashboardTemplates, errorsReport));
  }

  throwIfAnyErrors() {
    this.errorsReport.throwIfAny();
  }

  compileFile(file, errorsReport, cubes, exports, contexts, dashboardTemplates, toCompile, compiledFiles) {
    const self = this;
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
        cube: (name, cube) =>
          !cube ?
            this.cubeFactory({ ...name, fileName: file.fileName }) :
            cubes.push(Object.assign({}, cube, { name, fileName: file.fileName })),
        context: (name, context) => contexts.push(Object.assign({}, context, { name, fileName: file.fileName })),
        dashboardTemplate: (name, template) =>
          dashboardTemplates.push(Object.assign({}, template, { name, fileName: file.fileName })),
        addExport: (obj) => {
          exports[file.fileName] = exports[file.fileName] || {};
          exports[file.fileName] = Object.assign(exports[file.fileName], obj);
        },
        setExport: (obj) => {
          exports[file.fileName] = obj;
        },
        require: (extensionName) => {
          if (self.extensions[extensionName]) {
            return new (self.extensions[extensionName])(this.cubeFactory, self);
          } else {
            const foundFile = self.resolveModuleFile(file, extensionName, toCompile, errorsReport);
            self.compileFile(
              foundFile,
              errorsReport,
              cubes,
              exports,
              contexts,
              dashboardTemplates,
              toCompile,
              compiledFiles
            );
            exports[foundFile.fileName] = exports[foundFile.fileName] || {};
            return exports[foundFile.fileName];
          }
        }
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
      throw new UserError(`'${modulePath}' restricted`);
    }
    const packagePath = absPath.replace(nodeModulesPath, '').split('/').filter(s => !!s)[0];
    if (!packagePath) {
      throw new UserError(`'${modulePath}' is incorrect`);
    }
    if (!this.isWhiteListedPackage(packagePath)) {
      throw new UserError(`Package '${packagePath}' not found`);
    }
    if (fs.existsSync(absPath)) {
      const stat = fs.lstatSync(absPath);
      if (stat.isDirectory()) {
        absPath = path.resolve(absPath, 'index.js');
      }
    }
    absPath = path.extname(absPath) !== '.js' ? absPath + '.js' : absPath;
    if (!fs.existsSync(absPath)) {
      throw new UserError(`Path '${absPath.replace(nodeModulesPath + '/', '')}' not found`);
    }
    return this.readModuleFile(absPath, errorsReport);
  }

  readModuleFile(absPath, errorsReport) {
    const nodeModulesPath = path.resolve('node_modules');
    if (!moduleFileCache[absPath]) {
      const content = fs.readFileSync(absPath, 'utf-8');
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
      (packagePath.indexOf('-schema') === packagePath.length - "-schema".length);
  }
}

module.exports = DataSchemaCompiler;
