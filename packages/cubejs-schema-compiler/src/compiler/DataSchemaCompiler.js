import vm from 'vm';
import fs from 'fs';
import path from 'path';
import syntaxCheck from 'syntax-error';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';
import R from 'ramda';
import workerpool from 'workerpool';

import { getEnv, isNativeSupported } from '@cubejs-backend/shared';
import { transpileJs } from '@cubejs-backend/native';
import { UserError } from './UserError';
import { ErrorReporter } from './ErrorReporter';
import { CONTEXT_SYMBOLS } from './CubeSymbols';

const NATIVE_IS_SUPPORTED = isNativeSupported();

const moduleFileCache = {};

const JINJA_SYNTAX = /{%|%}|{{|}}/ig;

export class DataSchemaCompiler {
  constructor(repository, options = {}) {
    this.repository = repository;
    this.cubeCompilers = options.cubeCompilers || [];
    this.contextCompilers = options.contextCompilers || [];
    this.transpilers = options.transpilers || [];
    this.viewCompilers = options.viewCompilers || [];
    this.preTranspileCubeCompilers = options.preTranspileCubeCompilers || [];
    this.viewCompilationGate = options.viewCompilationGate;
    this.cubeNameCompilers = options.cubeNameCompilers || [];
    this.extensions = options.extensions || {};
    this.cubeDictionary = options.cubeDictionary;
    this.cubeSymbols = options.cubeSymbols;
    this.cubeFactory = options.cubeFactory;
    this.filesToCompile = options.filesToCompile;
    this.omitErrors = options.omitErrors;
    this.allowNodeRequire = options.allowNodeRequire;
    this.compileContext = options.compileContext;
    this.compilerCache = options.compilerCache;
    this.errorReport = options.errorReport;
    this.standalone = options.standalone;
    this.nativeInstance = options.nativeInstance;
    this.yamlCompiler = options.yamlCompiler;
    this.yamlCompiler.dataSchemaCompiler = this;
    this.pythonContext = null;
    this.workerPool = null;
    this.compilerId = options.compilerId;
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

  /**
   * @protected
   */
  async loadPythonContext(files, nsFileName) {
    const ns = files.find((f) => f.fileName === nsFileName);
    if (ns) {
      return this.nativeInstance.loadPythonContext(
        ns.fileName,
        ns.content
      );
    }

    return {
      filters: {},
      variables: {},
      functions: {}
    };
  }

  /**
   * @protected
   */
  async doCompile() {
    const files = await this.repository.dataSchemaFiles();

    this.pythonContext = await this.loadPythonContext(files, 'globals.py');
    this.yamlCompiler.initFromPythonContext(this.pythonContext);

    const toCompile = files.filter((f) => !this.filesToCompile || this.filesToCompile.indexOf(f.fileName) !== -1);

    const errorsReport = new ErrorReporter(null, [], this.errorReport);
    this.errorsReport = errorsReport;

    const transpilationWorkerThreads = getEnv('transpilationWorkerThreads');
    const transpilationNative = getEnv('transpilationNative');
    const { compilerId } = this;

    if (!transpilationNative && transpilationWorkerThreads) {
      const wc = getEnv('transpilationWorkerThreadsCount');
      this.workerPool = workerpool.pool(
        path.join(__dirname, 'transpilers/transpiler_worker'),
        wc > 0 ? { maxWorkers: wc } : undefined,
      );
    }

    const transpile = async () => {
      let cubeNames;
      let cubeSymbols;
      let transpilerNames;
      let results;

      if (transpilationNative || transpilationWorkerThreads) {
        cubeNames = Object.keys(this.cubeDictionary.byId);
        // We need only cubes and all its member names for transpiling.
        // Cubes doesn't change during transpiling, but are changed during compilation phase,
        // so we can prepare them once for every phase.
        // Communication between main and worker threads uses
        // The structured clone algorithm (@see https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Structured_clone_algorithm)
        // which doesn't allow passing any function objects, so we need to sanitize the symbols.
        // Communication with native backend also involves deserialization.
        cubeSymbols = Object.fromEntries(
          Object.entries(this.cubeSymbols.symbols)
            .map(
              ([key, value]) => [key, Object.fromEntries(
                Object.keys(value).map((k) => [k, true]),
              )],
            ),
        );

        // Transpilers are the same for all files within phase.
        transpilerNames = this.transpilers.map(t => t.constructor.name);
      }

      if (transpilationNative) {
        // Warming up swc compiler cache
        const dummyFile = {
          fileName: 'dummy.js',
          content: ';',
        };

        await this.transpileJsFile(dummyFile, errorsReport, { cubeNames, cubeSymbols, transpilerNames, contextSymbols: CONTEXT_SYMBOLS, compilerId });

        results = await Promise.all(toCompile.map(f => this.transpileFile(f, errorsReport, { transpilerNames, compilerId })));
      } else if (transpilationWorkerThreads) {
        results = await Promise.all(toCompile.map(f => this.transpileFile(f, errorsReport, { cubeNames, cubeSymbols, transpilerNames })));
      } else {
        results = await Promise.all(toCompile.map(f => this.transpileFile(f, errorsReport, {})));
      }

      return results.filter(f => !!f);
    };

    const compilePhase = async (compilers) => this.compileCubeFiles(compilers, await transpile(), errorsReport);

    return compilePhase({ cubeCompilers: this.cubeNameCompilers })
      .then(() => compilePhase({ cubeCompilers: this.preTranspileCubeCompilers.concat([this.viewCompilationGate]) }))
      .then(() => (this.viewCompilationGate.shouldCompileViews() ?
        compilePhase({ cubeCompilers: this.viewCompilers })
        : Promise.resolve()))
      .then(() => compilePhase({
        cubeCompilers: this.cubeCompilers,
        contextCompilers: this.contextCompilers,
      }))
      .then(() => {
        if (transpilationNative) {
          // Clean up cache
          const dummyFile = {
            fileName: 'terminate.js',
            content: ';',
          };

          return this.transpileJsFile(
            dummyFile,
            errorsReport,
            { cubeNames: [], cubeSymbols: {}, transpilerNames: [], contextSymbols: {}, compilerId: this.compilerId }
          );
        } else if (transpilationWorkerThreads && this.workerPool) {
          this.workerPool.terminate();
        }

        return Promise.resolve();
      });
  }

  compile() {
    if (!this.compilePromise) {
      this.compilePromise = this.doCompile().then((res) => {
        if (!this.omitErrors) {
          this.throwIfAnyErrors();
        }
        return res;
      });
    }

    return this.compilePromise;
  }

  async transpileFile(file, errorsReport, options) {
    if (R.endsWith('.jinja', file.fileName) ||
      (R.endsWith('.yml', file.fileName) || R.endsWith('.yaml', file.fileName))
      // TODO do Jinja syntax check with jinja compiler
      && file.content.match(JINJA_SYNTAX)
    ) {
      if (NATIVE_IS_SUPPORTED !== true) {
        throw new Error(
          `Native extension is required to process jinja files. ${NATIVE_IS_SUPPORTED.reason}. Read more: ` +
          'https://github.com/cube-js/cube/blob/master/packages/cubejs-backend-native/README.md#supported-architectures-and-platforms'
        );
      }

      this.yamlCompiler.getJinjaEngine().loadTemplate(file.fileName, file.content);

      return file;
    } else if (R.endsWith('.yml', file.fileName) || R.endsWith('.yaml', file.fileName)) {
      return file;
    } else if (R.endsWith('.js', file.fileName)) {
      return this.transpileJsFile(file, errorsReport, options);
    } else {
      return file;
    }
  }

  async transpileJsFile(file, errorsReport, { cubeNames, cubeSymbols, contextSymbols, transpilerNames, compilerId }) {
    try {
      if (getEnv('transpilationNative')) {
        const reqData = {
          fileName: file.fileName,
          transpilers: transpilerNames,
          compilerId,
          ...(cubeNames && {
            metaData: {
              cubeNames,
              cubeSymbols,
              contextSymbols,
            },
          }),
        };

        errorsReport.inFile(file);
        const res = await transpileJs(file.content, reqData);
        errorsReport.addErrors(res.errors);
        errorsReport.addWarnings(res.warnings);
        errorsReport.exitFile();

        return Object.assign({}, file, { content: res.code });
      } else if (getEnv('transpilationWorkerThreads')) {
        const data = {
          fileName: file.fileName,
          content: file.content,
          transpilers: transpilerNames,
          cubeNames,
          cubeSymbols,
        };

        const res = await this.workerPool.exec('transpile', [data]);
        errorsReport.addErrors(res.errors);
        errorsReport.addWarnings(res.warnings);

        return Object.assign({}, file, { content: res.content });
      } else {
        const ast = parse(
          file.content,
          {
            sourceFilename: file.fileName,
            sourceType: 'module',
            plugins: ['objectRestSpread'],
          },
        );

        errorsReport.inFile(file);
        this.transpilers.forEach((t) => {
          babelTraverse(ast, t.traverseObject(errorsReport));
        });
        errorsReport.exitFile();

        const content = babelGenerator(ast, {}, file.content).code;
        return Object.assign({}, file, { content });
      }
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

    if (R.endsWith('.js', file.fileName)) {
      this.compileJsFile(file, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles);
    } else if (R.endsWith('.yml.jinja', file.fileName) || R.endsWith('.yaml.jinja', file.fileName) ||
      (
        R.endsWith('.yml', file.fileName) || R.endsWith('.yaml', file.fileName)
        // TODO do Jinja syntax check with jinja compiler
      ) && file.content.match(JINJA_SYNTAX)
    ) {
      asyncModules.push(() => this.yamlCompiler.compileYamlWithJinjaFile(
        file,
        errorsReport,
        cubes,
        contexts,
        exports,
        asyncModules,
        toCompile,
        compiledFiles,
        this.standalone ? {} : this.cloneCompileContextWithGetterAlias(this.compileContext),
        this.pythonContext
      ));
    } else if (R.endsWith('.yml', file.fileName) || R.endsWith('.yaml', file.fileName)) {
      this.yamlCompiler.compileYamlFile(file, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles);
    }
  }

  compileJsFile(file, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles) {
    const err = syntaxCheck(file.content, file.fileName);
    if (err) {
      errorsReport.error(err.toString());
    }

    try {
      vm.runInNewContext(file.content, {
        view: (name, cube) => (
          !cube ?
            this.cubeFactory({ ...name, fileName: file.fileName, isView: true }) :
            cubes.push(Object.assign({}, cube, { name, fileName: file.fileName, isView: true }))
        ),
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
            return new (this.extensions[extensionName])(this.cubeFactory, this, cubes);
          } else {
            const foundFile = this.resolveModuleFile(file, extensionName, toCompile, errorsReport);
            if (!foundFile && this.allowNodeRequire) {
              if (extensionName.indexOf('.') === 0) {
                extensionName = path.resolve(this.repository.localPath(), extensionName);
              }
              // eslint-disable-next-line global-require,import/no-dynamic-require
              const Extension = require(extensionName);
              if (Object.getPrototypeOf(Extension).name === 'AbstractExtension') {
                return new Extension(this.cubeFactory, this, cubes);
              }
              return Extension;
            }
            this.compileFile(
              foundFile,
              errorsReport,
              cubes,
              exports,
              contexts,
              toCompile,
              compiledFiles,
            );
            exports[foundFile.fileName] = exports[foundFile.fileName] || {};
            return exports[foundFile.fileName];
          }
        },
        COMPILE_CONTEXT: this.standalone ? this.standaloneCompileContextProxy() : this.cloneCompileContextWithGetterAlias(this.compileContext || {}),
      }, { filename: file.fileName, timeout: 15000 });
    } catch (e) {
      errorsReport.error(e);
    }
  }

  // Alias "securityContext" with "security_context" (snake case version)
  // to support snake case based data models
  cloneCompileContextWithGetterAlias(compileContext) {
    const clone = R.clone(compileContext || {});
    clone.security_context = compileContext.securityContext;
    return clone;
  }

  standaloneCompileContextProxy() {
    return new Proxy({}, {
      get: () => {
        throw new UserError('COMPILE_CONTEXT can\'t be used unless contextToAppId is defined. Please see https://cube.dev/docs/config#options-reference-context-to-app-id.');
      }
    });
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
