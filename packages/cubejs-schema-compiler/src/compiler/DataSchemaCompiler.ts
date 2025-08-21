import { AsyncLocalStorage } from 'async_hooks';
import crypto from 'crypto';
import vm from 'vm';
import fs from 'fs';
import os from 'os';
import path from 'path';
import syntaxCheck from 'syntax-error';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';
import R from 'ramda';
import workerpool from 'workerpool';
import { LRUCache } from 'lru-cache';

import { FileContent, getEnv, isNativeSupported, SchemaFileRepository } from '@cubejs-backend/shared';
import { NativeInstance, PythonCtx, transpileJs } from '@cubejs-backend/native';
import { UserError } from './UserError';
import { ErrorReporter, ErrorReporterOptions, SyntaxErrorInterface } from './ErrorReporter';
import { CONTEXT_SYMBOLS, CubeDefinition, CubeSymbols } from './CubeSymbols';
import { ViewCompilationGate } from './ViewCompilationGate';
import { TranspilerInterface } from './transpilers';
import { CompilerInterface } from './PrepareCompiler';
import { YamlCompiler } from './YamlCompiler';
import { CubeDictionary } from './CubeDictionary';
import { CompilerCache } from './CompilerCache';

const ctxFileStorage = new AsyncLocalStorage<FileContent>();

const NATIVE_IS_SUPPORTED = isNativeSupported();

const moduleFileCache = {};

const JINJA_SYNTAX = /{%|%}|{{|}}/ig;

const getThreadsCount = () => {
  const envThreads = getEnv('transpilationWorkerThreadsCount');
  if (envThreads > 0) {
    return envThreads;
  }

  const cpuCount = os.cpus()?.length;
  if (cpuCount) {
    // there's no practical boost above 5 threads even if you have more cores.
    return Math.min(Math.max(1, cpuCount - 1), 5);
  }

  return 3; // Default (like the workerpool do)
};

export type DataSchemaCompilerOptions = {
  compilerCache: CompilerCache;
  omitErrors?: boolean;
  extensions?: Record<string, any>;
  filesToCompile?: string[];
  nativeInstance: NativeInstance;
  cubeFactory: Function;
  cubeDictionary: CubeDictionary;
  cubeSymbols: CubeSymbols;
  cubeCompilers?: CompilerInterface[];
  contextCompilers?: CompilerInterface[];
  transpilers?: TranspilerInterface[];
  viewCompilers?: CompilerInterface[];
  viewCompilationGate: ViewCompilationGate;
  cubeNameCompilers?: CompilerInterface[];
  preTranspileCubeCompilers?: CompilerInterface[];
  yamlCompiler: YamlCompiler;
  errorReport?: ErrorReporterOptions;
  compilerId?: string;
  standalone?: boolean;
  compileContext?: any;
  allowNodeRequire?: boolean;
  compiledScriptCache: LRUCache<string, vm.Script>;
};

export type TranspileOptions = {
  cubeNames?: string[];
  cubeSymbols?: Record<string, Record<string, boolean>>;
  contextSymbols?: Record<string, string>;
  transpilerNames?: string[];
  compilerId?: string;
  stage?: 0 | 1 | 2 | 3;
};

export type CompileStage = 0 | 1 | 2 | 3;

type CompileCubeFilesCompilers = {
  cubeCompilers?: CompilerInterface[];
  contextCompilers?: CompilerInterface[];
};

export class DataSchemaCompiler {
  private readonly repository: SchemaFileRepository;

  private readonly cubeCompilers: CompilerInterface[];

  private readonly contextCompilers: CompilerInterface[];

  private readonly transpilers: TranspilerInterface[];

  private readonly viewCompilers: CompilerInterface[];

  private readonly preTranspileCubeCompilers: CompilerInterface[];

  private readonly viewCompilationGate: ViewCompilationGate;

  private readonly cubeNameCompilers: CompilerInterface[];

  private readonly extensions: Record<string, any>;

  private readonly cubeDictionary: CubeDictionary;

  private readonly cubeSymbols: CubeSymbols;

  // Actually should be something like
  // createCube(cubeDefinition: CubeDefinition): CubeDefinitionExtended
  private readonly cubeFactory: CallableFunction;

  private readonly filesToCompile: string[];

  private readonly omitErrors: boolean;

  private readonly allowNodeRequire: boolean;

  private readonly compilerCache: CompilerCache;

  private readonly compileContext: any;

  private errorReportOptions: ErrorReporterOptions | undefined;

  private errorsReporter: ErrorReporter | undefined;

  private readonly standalone: boolean;

  private readonly nativeInstance: NativeInstance;

  private readonly yamlCompiler: YamlCompiler;

  private pythonContext: PythonCtx | null;

  private workerPool: workerpool.Pool | null;

  private readonly compilerId: string;

  private readonly compiledScriptCache: LRUCache<string, vm.Script>;

  private compileV8ContextCache: vm.Context | null = null;

  // FIXME: Is public only because of tests, should be private
  public compilePromise: any;

  private currentQuery: any;

  public constructor(repository: SchemaFileRepository, options: DataSchemaCompilerOptions) {
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
    this.filesToCompile = options.filesToCompile || [];
    this.omitErrors = options.omitErrors || false;
    this.allowNodeRequire = options.allowNodeRequire || false;
    this.compileContext = options.compileContext;
    this.compilerCache = options.compilerCache;
    this.errorReportOptions = options.errorReport;
    this.standalone = options.standalone || false;
    this.nativeInstance = options.nativeInstance;
    this.yamlCompiler = options.yamlCompiler;
    this.yamlCompiler.dataSchemaCompiler = this;
    this.pythonContext = null;
    this.workerPool = null;
    this.compilerId = options.compilerId || 'default';
    this.compiledScriptCache = options.compiledScriptCache;
  }

  public compileObjects(compileServices, objects, errorsReport: ErrorReporter) {
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

  protected async loadPythonContext(files, nsFileName) {
    const ns = files.find((f) => f.fileName === nsFileName);
    if (ns) {
      return this.nativeInstance.loadPythonContext(
        ns.fileName,
        ns.content
      );
    }

    return {
      __type: 'PythonCtx',
      filters: {},
      variables: {},
      functions: {}
    } as PythonCtx;
  }

  protected async doCompile() {
    const files = await this.repository.dataSchemaFiles();

    this.pythonContext = await this.loadPythonContext(files, 'globals.py');
    this.yamlCompiler.initFromPythonContext(this.pythonContext);

    const toCompile = files.filter((f) => !this.filesToCompile || !this.filesToCompile.length || this.filesToCompile.indexOf(f.fileName) !== -1);

    const errorsReport = new ErrorReporter(null, [], this.errorReportOptions);
    this.errorsReporter = errorsReport;

    const transpilationWorkerThreads = getEnv('transpilationWorkerThreads');
    const transpilationNative = getEnv('transpilationNative');
    const transpilationNativeThreadsCount = getThreadsCount();
    const { compilerId } = this;

    if (!transpilationNative && transpilationWorkerThreads) {
      const wc = getEnv('transpilationWorkerThreadsCount');
      this.workerPool = workerpool.pool(
        path.join(__dirname, 'transpilers/transpiler_worker'),
        wc > 0 ? { maxWorkers: wc } : undefined,
      );
    }

    const transpile = async (stage: CompileStage): Promise<FileContent[]> => {
      let cubeNames: string[] = [];
      let cubeSymbols: Record<string, Record<string, boolean>> = {};
      let transpilerNames: string[] = [];
      let results: (FileContent | undefined)[];

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
          Object.entries(this.cubeSymbols.symbols as Record<string, Record<string, any>>)
            .map(
              ([key, value]: [string, Record<string, any>]) => [key, Object.fromEntries(
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

        await this.transpileJsFile(dummyFile, errorsReport, { cubeNames, cubeSymbols, transpilerNames, contextSymbols: CONTEXT_SYMBOLS, compilerId, stage });

        const nonJsFilesTasks = toCompile.filter(file => !file.fileName.endsWith('.js'))
          .map(f => this.transpileFile(f, errorsReport, { transpilerNames, compilerId }));

        const jsFiles = toCompile.filter(file => file.fileName.endsWith('.js'));
        let JsFilesTasks = [];

        if (jsFiles.length > 0) {
          let jsChunks;
          if (jsFiles.length < transpilationNativeThreadsCount * transpilationNativeThreadsCount) {
            jsChunks = [jsFiles];
          } else {
            const baseSize = Math.floor(jsFiles.length / transpilationNativeThreadsCount);
            jsChunks = [];
            for (let i = 0; i < transpilationNativeThreadsCount; i++) {
              // For the last part, we take the remaining files so we don't lose the extra ones.
              const start = i * baseSize;
              const end = (i === transpilationNativeThreadsCount - 1) ? jsFiles.length : start + baseSize;
              jsChunks.push(jsFiles.slice(start, end));
            }
          }
          JsFilesTasks = jsChunks.map(chunk => this.transpileJsFilesBulk(chunk, errorsReport, { transpilerNames, compilerId }));
        }

        results = (await Promise.all([...nonJsFilesTasks, ...JsFilesTasks])).flat();
      } else if (transpilationWorkerThreads) {
        results = await Promise.all(toCompile.map(f => this.transpileFile(f, errorsReport, { cubeNames, cubeSymbols, transpilerNames })));
      } else {
        results = await Promise.all(toCompile.map(f => this.transpileFile(f, errorsReport, {})));
      }

      return results.filter(f => !!f) as FileContent[];
    };

    let cubes: CubeDefinition[] = [];
    let exports: Record<string, Record<string, any>> = {};
    let contexts: Record<string, any>[] = [];
    let compiledFiles: Record<string, boolean> = {};
    let asyncModules: CallableFunction[] = [];
    let transpiledFiles: FileContent[] = [];

    this.compileV8ContextCache = vm.createContext({
      view: (name, cube) => {
        const file = ctxFileStorage.getStore();
        if (!file) {
          throw new Error('No file stored in context');
        }
        return !cube ?
          this.cubeFactory({ ...name, fileName: file.fileName, isView: true }) :
          cubes.push({ ...cube, name, fileName: file.fileName, isView: true });
      },
      cube: (name, cube) => {
        const file = ctxFileStorage.getStore();
        if (!file) {
          throw new Error('No file stored in context');
        }
        return !cube ?
          this.cubeFactory({ ...name, fileName: file.fileName }) :
          cubes.push({ ...cube, name, fileName: file.fileName });
      },
      context: (name: string, context) => {
        const file = ctxFileStorage.getStore();
        if (!file) {
          throw new Error('No file stored in context');
        }
        return contexts.push({ ...context, name, fileName: file.fileName });
      },
      addExport: (obj) => {
        const file = ctxFileStorage.getStore();
        if (!file) {
          throw new Error('No file stored in context');
        }
        exports[file.fileName] = exports[file.fileName] || {};
        exports[file.fileName] = Object.assign(exports[file.fileName], obj);
      },
      setExport: (obj) => {
        const file = ctxFileStorage.getStore();
        if (!file) {
          throw new Error('No file stored in context');
        }
        exports[file.fileName] = obj;
      },
      asyncModule: (fn) => {
        const file = ctxFileStorage.getStore();
        if (!file) {
          throw new Error('No file stored in context');
        }
        // We need to run async module code in the context of the original data model file
        // where it was defined. So we pass the same file to the async context.
        // @see https://nodejs.org/api/async_context.html#class-asynclocalstorage
        asyncModules.push(async () => ctxFileStorage.run(file, () => fn()));
      },
      require: (extensionName: string) => {
        const file = ctxFileStorage.getStore();
        if (!file) {
          throw new Error('No file stored in context');
        }

        if (this.extensions[extensionName]) {
          return new (this.extensions[extensionName])(this.cubeFactory, this, cubes);
        } else {
          const foundFile = this.resolveModuleFile(file, extensionName, transpiledFiles, errorsReport);
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
            compiledFiles,
            [],
            { doSyntaxCheck: true }
          );
          exports[foundFile.fileName] = exports[foundFile.fileName] || {};
          return exports[foundFile.fileName];
        }
      },
      COMPILE_CONTEXT: this.standalone ? this.standaloneCompileContextProxy() : this.cloneCompileContextWithGetterAlias(this.compileContext || {}),
    });

    const compilePhase = async (compilers: CompileCubeFilesCompilers, stage: 0 | 1 | 2 | 3) => {
      // clear the objects for the next phase
      cubes = [];
      exports = {};
      contexts = [];
      compiledFiles = {};
      asyncModules = [];
      transpiledFiles = await transpile(stage);

      return this.compileCubeFiles(cubes, contexts, compiledFiles, asyncModules, compilers, transpiledFiles, errorsReport);
    };

    return compilePhase({ cubeCompilers: this.cubeNameCompilers }, 0)
      .then(() => compilePhase({ cubeCompilers: this.preTranspileCubeCompilers.concat([this.viewCompilationGate]) }, 1))
      .then(() => (this.viewCompilationGate.shouldCompileViews() ?
        compilePhase({ cubeCompilers: this.viewCompilers }, 2)
        : Promise.resolve()))
      .then(() => compilePhase({
        cubeCompilers: this.cubeCompilers,
        contextCompilers: this.contextCompilers,
      }, 3))
      .then(() => {
        // Free unneeded resources
        cubes = [];
        exports = {};
        contexts = [];
        compiledFiles = {};
        asyncModules = [];
        transpiledFiles = [];

        if (transpilationNative) {
          // Clean up cache
          const dummyFile = {
            fileName: 'terminate.js',
            content: ';',
          };

          this.transpileJsFile(
            dummyFile,
            errorsReport,
            { cubeNames: [], cubeSymbols: {}, transpilerNames: [], contextSymbols: {}, compilerId: this.compilerId, stage: 0 }
          ).then(() => undefined);
        } else if (transpilationWorkerThreads && this.workerPool) {
          this.workerPool.terminate();
        }
      });
  }

  public compile() {
    if (!this.compilePromise) {
      this.compilePromise = this.doCompile().then((res) => {
        if (!this.omitErrors) {
          this.throwIfAnyErrors();
        }
        // Free unneeded resources
        this.compileV8ContextCache = null;
        this.cubeDictionary.free();
        this.cubeSymbols.free();
        return res;
      });
    }

    return this.compilePromise;
  }

  private async transpileFile(
    file: FileContent,
    errorsReport: ErrorReporter,
    options: TranspileOptions = {}
  ): Promise<(FileContent | undefined)> {
    if (file.fileName.endsWith('.jinja') ||
      (file.fileName.endsWith('.yml') || file.fileName.endsWith('.yaml'))
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
    } else if (file.fileName.endsWith('.yml') || file.fileName.endsWith('.yaml')) {
      return file;
    } else if (file.fileName.endsWith('.js')) {
      return this.transpileJsFile(file, errorsReport, options);
    } else {
      return file;
    }
  }

  /**
   * Right now it is used only for transpilation in native,
   * so no checks for transpilation type inside this method
   */
  private async transpileJsFilesBulk(
    files: FileContent[],
    errorsReport: ErrorReporter,
    { cubeNames, cubeSymbols, contextSymbols, transpilerNames, compilerId, stage }: TranspileOptions
  ): Promise<(FileContent | undefined)[]> {
    // for bulk processing this data may be optimized even more by passing transpilerNames, compilerId only once for a bulk
    // but this requires more complex logic to be implemented in the native side.
    // And comparing to the file content sizes, a few bytes of JSON data is not a big deal here
    const reqDataArr = files.map(file => ({
      fileName: file.fileName,
      fileContent: file.content,
      transpilers: transpilerNames || [],
      compilerId: compilerId || '',
      ...(cubeNames && {
        metaData: {
          cubeNames,
          cubeSymbols: cubeSymbols || {},
          contextSymbols: contextSymbols || {},
          stage: stage || 0 as CompileStage,
        },
      }),
    }));
    const res = await transpileJs(reqDataArr);

    return files.map((file, index) => {
      errorsReport.inFile(file);
      if (!res[index]) { // This should not happen in theory but just to be safe
        errorsReport.error(`No transpilation result received for the file ${file.fileName}.`);
        return undefined;
      }
      errorsReport.addErrors(res[index].errors);
      errorsReport.addWarnings(res[index].warnings as unknown as SyntaxErrorInterface[]);
      errorsReport.exitFile();

      return { ...file, content: res[index].code };
    });
  }

  private async transpileJsFile(
    file: FileContent,
    errorsReport: ErrorReporter,
    { cubeNames, cubeSymbols, contextSymbols, transpilerNames, compilerId, stage }: TranspileOptions
  ): Promise<(FileContent | undefined)> {
    try {
      if (getEnv('transpilationNative')) {
        const reqData = {
          fileName: file.fileName,
          fileContent: file.content,
          transpilers: transpilerNames || [],
          compilerId: compilerId || '',
          ...(cubeNames && {
            metaData: {
              cubeNames,
              cubeSymbols: cubeSymbols || {},
              contextSymbols: contextSymbols || {},
              stage: stage || 0 as CompileStage,
            },
          }),
        };

        errorsReport.inFile(file);
        const res = await transpileJs([reqData]);
        errorsReport.addErrors(res[0].errors);
        errorsReport.addWarnings(res[0].warnings as unknown as SyntaxErrorInterface[]);
        errorsReport.exitFile();

        return { ...file, content: res[0].code };
      } else if (getEnv('transpilationWorkerThreads')) {
        const data = {
          fileName: file.fileName,
          content: file.content,
          transpilers: transpilerNames,
          cubeNames,
          cubeSymbols,
        };

        const res = await this.workerPool!.exec('transpile', [data]);
        errorsReport.addErrors(res.errors);
        errorsReport.addWarnings(res.warnings);

        return { ...file, content: res.content };
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
        return { ...file, content };
      }
    } catch (e: any) {
      if (e.toString().indexOf('SyntaxError') !== -1) {
        const err = e as SyntaxErrorInterface;
        const line = file.content.split('\n')[(err.loc?.start?.line || 1) - 1];
        const spaces = Array(err.loc?.start.column).fill(' ').join('');
        errorsReport.error(`Syntax error during '${file.fileName}' parsing: ${err.message}:\n${line}\n${spaces}^`);
      } else {
        errorsReport.error(e);
      }
    }
    return undefined;
  }

  public withQuery(query, fn) {
    const oldQuery = this.currentQuery;
    this.currentQuery = query;
    try {
      return fn();
    } finally {
      this.currentQuery = oldQuery;
    }
  }

  public contextQuery() {
    return this.currentQuery;
  }

  private async compileCubeFiles(
    cubes: CubeDefinition[],
    contexts: Record<string, any>[],
    compiledFiles: Record<string, boolean>,
    asyncModules: CallableFunction[],
    compilers: CompileCubeFilesCompilers,
    toCompile: FileContent[],
    errorsReport: ErrorReporter
  ) {
    toCompile
      .forEach((file) => {
        this.compileFile(
          file,
          errorsReport,
          compiledFiles,
          asyncModules
        );
      });
    await asyncModules.reduce((a: Promise<void>, b: CallableFunction) => a.then(() => b()), Promise.resolve());
    return this.compileObjects(compilers.cubeCompilers || [], cubes, errorsReport)
      .then(() => this.compileObjects(compilers.contextCompilers || [], contexts, errorsReport));
  }

  public throwIfAnyErrors() {
    this.errorsReporter?.throwIfAny();
  }

  private compileFile(
    file: FileContent,
    errorsReport: ErrorReporter,
    compiledFiles: Record<string, boolean>,
    asyncModules: CallableFunction[],
    { doSyntaxCheck } = { doSyntaxCheck: false }
  ) {
    if (compiledFiles[file.fileName]) {
      return;
    }

    compiledFiles[file.fileName] = true;

    if (file.fileName.endsWith('.js')) {
      this.compileJsFile(file, errorsReport, { doSyntaxCheck });
    } else if (file.fileName.endsWith('.yml.jinja') || file.fileName.endsWith('.yaml.jinja') ||
      (
        file.fileName.endsWith('.yml') || file.fileName.endsWith('.yaml')
        // TODO do Jinja syntax check with jinja compiler
      ) && file.content.match(JINJA_SYNTAX)
    ) {
      asyncModules.push(() => this.yamlCompiler.compileYamlWithJinjaFile(
        file,
        errorsReport,
        this.standalone ? {} : this.cloneCompileContextWithGetterAlias(this.compileContext),
        this.pythonContext!
      ));
    } else if (file.fileName.endsWith('.yml') || file.fileName.endsWith('.yaml')) {
      this.yamlCompiler.compileYamlFile(file, errorsReport);
    }
  }

  private getJsScript(file: FileContent): vm.Script {
    const cacheKey = crypto.createHash('md5').update(JSON.stringify(file.content)).digest('hex');

    if (this.compiledScriptCache.has(cacheKey)) {
      return this.compiledScriptCache.get(cacheKey)!;
    }

    const script = new vm.Script(file.content, { filename: file.fileName });
    this.compiledScriptCache.set(cacheKey, script);
    return script;
  }

  public compileJsFile(
    file: FileContent,
    errorsReport: ErrorReporter,
    { doSyntaxCheck } = { doSyntaxCheck: false }
  ) {
    if (doSyntaxCheck) {
      // There is no need to run syntax check for data model files
      // because they were checked during transpilation/transformation phase
      // Only external files (included modules) might need syntax check
      const err = syntaxCheck(file.content, file.fileName);
      if (err) {
        errorsReport.error(err.toString());
      }
    }

    try {
      const script = this.getJsScript(file);

      // We use AsyncLocalStorage to store the current file context
      // so that it can be accessed in the script execution context even within async functions.
      // @see https://nodejs.org/api/async_context.html#class-asynclocalstorage
      ctxFileStorage.run(file, () => {
        script.runInContext(this.compileV8ContextCache!, { timeout: 15000 });
      });
    } catch (e) {
      errorsReport.error(e);
    }
  }

  // Alias "securityContext" with "security_context" (snake case version)
  // to support snake case based data models
  private cloneCompileContextWithGetterAlias(compileContext) {
    const ctx = compileContext || {};
    const clone = R.clone(ctx);
    clone.security_context = ctx.securityContext;
    return clone;
  }

  private standaloneCompileContextProxy() {
    return new Proxy({}, {
      get: () => {
        throw new UserError('COMPILE_CONTEXT can\'t be used unless contextToAppId is defined. Please see https://cube.dev/docs/config#options-reference-context-to-app-id.');
      }
    });
  }

  private resolveModuleFile(currentFile: FileContent, modulePath: string, toCompile: FileContent[], errorsReport: ErrorReporter) {
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

    if (!absPath.startsWith(nodeModulesPath)) {
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

  private readModuleFile(absPath: string, errorsReport: ErrorReporter) {
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

  private isWhiteListedPackage(packagePath: string): boolean {
    return packagePath.indexOf('-schema') !== -1 &&
      (packagePath.indexOf('-schema') === packagePath.length - '-schema'.length);
  }
}
