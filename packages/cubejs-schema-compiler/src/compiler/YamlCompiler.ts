import YAML from 'js-yaml';
import * as t from '@babel/types';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';
import { JinjaEngine, NativeInstance, PythonCtx } from '@cubejs-backend/native';

import type { FileContent } from '@cubejs-backend/shared';

import { getEnv } from '@cubejs-backend/shared';
import {
  CubePropContextTranspiler,
  transpiledFields,
  transpiledFieldsPatterns,
  TranspilerCubeResolver, TranspilerSymbolResolver
} from './transpilers';
import { PythonParser } from '../parser/PythonParser';
import { nonStringFields } from './CubeValidator';
import { ErrorReporter } from './ErrorReporter';
import { camelizeCube } from './utils';
import { CompileContext } from './DataSchemaCompiler';

type EscapeStateStack = {
  inFormattedStr?: boolean;
  inStr?: boolean;
  inTemplate?: boolean;
  depth?: number;
};

export class YamlCompiler {
  protected jinjaEngine: JinjaEngine | null = null;

  public constructor(
    private readonly cubeSymbols: TranspilerSymbolResolver,
    private readonly cubeDictionary: TranspilerCubeResolver,
    private readonly nativeInstance: NativeInstance,
    private readonly viewCompiler: TranspilerSymbolResolver,
  ) {
  }

  public free() {
    this.jinjaEngine = null;
  }

  public getJinjaEngine(): JinjaEngine {
    if (this.jinjaEngine) {
      return this.jinjaEngine;
    }

    throw new Error('Jinja engine was not initialized');
  }

  public initFromPythonContext(ctx: PythonCtx) {
    this.jinjaEngine = this.nativeInstance.newJinjaEngine({
      debugInfo: getEnv('devMode'),
      filters: ctx.filters,
      workers: 1,
    });
  }

  public async renderTemplate(file: FileContent, compileContext: CompileContext, pythonContext: PythonCtx): Promise<FileContent> {
    return {
      fileName: file.fileName,
      content: await this.getJinjaEngine().renderTemplate(file.fileName, compileContext, {
        ...pythonContext.functions,
        ...pythonContext.variables
      }),
    };
  }

  public async compileYamlWithJinjaFile(
    file: FileContent,
    errorsReport: ErrorReporter,
    compileContext: CompileContext,
    pythonContext: PythonCtx
  ): Promise<FileContent | undefined> {
    const renderedFile = await this.renderTemplate(file, compileContext, pythonContext);

    return this.transpileYamlFile(renderedFile, errorsReport);
  }

  public transpileYamlFile(
    file: FileContent,
    errorsReport: ErrorReporter,
  ): FileContent | undefined {
    if (!file.content.trim()) {
      return;
    }

    const yamlObj: any = YAML.load(file.content);
    if (!yamlObj) {
      return;
    }

    const transpiledFilesContent: string[] = [];

    for (const key of Object.keys(yamlObj)) {
      if (key === 'cubes') {
        this.checkDuplicateNames(yamlObj.cubes || [], errorsReport, (name) => `Found duplicate cube name '${name}'.`);

        (yamlObj.cubes || []).forEach(({ name, ...cube }) => {
          const transpiledCube = this.transpileAndPrepareJsFile('cube', { name, ...cube }, errorsReport);
          transpiledFilesContent.push(transpiledCube);
        });
      } else if (key === 'views') {
        this.checkDuplicateNames(yamlObj.views || [], errorsReport, (name) => `Found duplicate view name '${name}'.`);

        (yamlObj.views || []).forEach(({ name, ...cube }) => {
          const transpiledView = this.transpileAndPrepareJsFile('view', { name, ...cube }, errorsReport);
          transpiledFilesContent.push(transpiledView);
        });
      } else {
        errorsReport.error(`Unexpected YAML key: ${key}. Only 'cubes' and 'views' are allowed here.`);
      }
    }

    // eslint-disable-next-line consistent-return
    return {
      fileName: file.fileName,
      content: transpiledFilesContent.join('\n\n'),
    } as FileContent;
  }

  private transpileAndPrepareJsFile(methodFn: ('cube' | 'view'), cubeObj, errorsReport: ErrorReporter): string {
    const yamlAst = this.transformYamlCubeObj(cubeObj, errorsReport);

    const cubeOrViewCall = t.callExpression(t.identifier(methodFn), [t.stringLiteral(cubeObj.name), yamlAst]);

    return babelGenerator(cubeOrViewCall, {}, '').code;
  }

  private transformYamlCubeObj(cubeObj, errorsReport: ErrorReporter) {
    camelizeCube(cubeObj);

    const ctx = { cubeName: cubeObj.name };
    cubeObj.measures = this.yamlArrayToObj(cubeObj.measures || [], 'measure', errorsReport, ctx);
    cubeObj.dimensions = this.yamlArrayToObj(cubeObj.dimensions || [], 'dimension', errorsReport, ctx);
    cubeObj.segments = this.yamlArrayToObj(cubeObj.segments || [], 'segment', errorsReport, ctx);
    cubeObj.preAggregations = this.yamlArrayToObj(cubeObj.preAggregations || [], 'preAggregation', errorsReport, ctx);
    cubeObj.hierarchies = this.yamlArrayToObj(cubeObj.hierarchies || [], 'hierarchies', errorsReport, ctx);

    cubeObj.joins = cubeObj.joins || []; // For edge cases where joins are not defined/null

    if (!Array.isArray(cubeObj.joins)) {
      errorsReport.error('joins must be defined as array');
      cubeObj.joins = [];
    }

    return this.transpileYaml(cubeObj, [], cubeObj.name, errorsReport);
  }

  private transpileYaml(obj, propertyPath, cubeName, errorsReport: ErrorReporter) {
    if (transpiledFields.has(propertyPath[propertyPath.length - 1])) {
      for (const p of transpiledFieldsPatterns) {
        const fullPath = propertyPath.join('.');
        if (fullPath.match(p)) {
          if (typeof obj === 'string' && ['sql', 'sqlTable'].includes(propertyPath[propertyPath.length - 1])) {
            return this.parsePythonIntoArrowFunction(`f"${this.escapeDoubleQuotes(obj)}"`, cubeName, obj, errorsReport);
          } else if (typeof obj === 'string') {
            return this.parsePythonIntoArrowFunction(obj, cubeName, obj, errorsReport);
          } else if (Array.isArray(obj)) {
            const resultAst = t.program([t.expressionStatement(t.arrayExpression(obj.map(code => {
              let ast: t.Program | t.NullLiteral | t.BooleanLiteral | t.NumericLiteral | null = null;
              // Special case for accessPolicy.rowLevel.filter.values and other values-like fields
              if (propertyPath[propertyPath.length - 1] === 'values') {
                if (typeof code === 'string') {
                  ast = this.parsePythonAndTranspileToJs(`f"${this.escapeDoubleQuotes(code)}"`, errorsReport);
                } else if (typeof code === 'boolean') {
                  ast = t.booleanLiteral(code);
                } else if (typeof code === 'number') {
                  ast = t.numericLiteral(code);
                } else if (code instanceof Date) {
                  // Special case when dates are defined in YAML as strings without quotes
                  // YAML parser treats them as Date objects, but for conversion we need them as strings
                  ast = this.parsePythonAndTranspileToJs(`f"${this.escapeDoubleQuotes(code.toISOString())}"`, errorsReport);
                }
              }
              if (ast === null) {
                ast = this.parsePythonAndTranspileToJs(code, errorsReport);
              }
              return this.extractProgramBodyIfNeeded(ast);
            }).filter(ast => !!ast)))]);
            return this.astIntoArrowFunction(resultAst, '', cubeName);
          }
        }
      }
    }

    if (propertyPath[propertyPath.length - 1] === 'extends') {
      const ast = this.parsePythonAndTranspileToJs(obj, errorsReport);
      return this.astIntoArrowFunction(ast, obj, cubeName, name => this.cubeDictionary.resolveCube(name));
    } else if (typeof obj === 'string') {
      let code = obj;

      if (!nonStringFields.has(propertyPath[propertyPath.length - 1])) {
        code = `f"${this.escapeDoubleQuotes(obj)}"`;
      }

      const ast = this.parsePythonAndTranspileToJs(code, errorsReport);
      return this.extractProgramBodyIfNeeded(ast);
    } else if (typeof obj === 'boolean') {
      return t.booleanLiteral(obj);
    } else if (typeof obj === 'number') {
      return t.numericLiteral(obj);
    } else if (obj === null && propertyPath.includes('meta')) {
      return t.nullLiteral();
    }

    if (typeof obj === 'object' && obj !== null) {
      if (Array.isArray(obj)) {
        return t.arrayExpression(obj.map((value, i) => this.transpileYaml(value, propertyPath.concat(i.toString()), cubeName, errorsReport)));
      } else {
        const properties: any[] = [];

        for (const propKey of Object.keys(obj)) {
          const ast = this.transpileYaml(obj[propKey], propertyPath.concat(propKey), cubeName, errorsReport);
          properties.push(t.objectProperty(t.stringLiteral(propKey), ast));
        }

        return t.objectExpression(properties);
      }
    } else {
      throw new Error(`Unexpected input during yaml transpiling: ${JSON.stringify(obj)}`);
    }
  }

  private escapeDoubleQuotes(str: string): string {
    const result: string[] = [];
    const stateStack: EscapeStateStack[] = [];
    const peek = () => stateStack[stateStack.length - 1] || { inStr: true, inFormattedStr: true };
    for (let i = 0; i < str.length; i++) {
      if (str[i] === 'f' && str[i + 1] === '"' && !peek().inStr) {
        i += 1;
        result.push('f"');
        stateStack.push({ inFormattedStr: true, inStr: true });
      } else if (str[i] === '"' && !peek().inStr) {
        result.push('"');
        stateStack.push({ inStr: true });
      } else if (str[i] === '"' && stateStack.length === 0) {
        result.push('\\"');
      } else if (str[i] === '"' && peek().inStr) {
        result.push(str[i]);
        stateStack.pop();
      } else if (str[i] === '`' && stateStack.length === 0) {
        result.push('\\`');
      } else if (str[i] === '`' && peek().inStr) {
        result.push(str[i]);
        stateStack.pop();
      } else if (str[i] === '\\' && str[i + 1] === '{' && stateStack.length === 0) {
        result.push('\\{');
        i += 1;
      } else if (str[i] === '\\' && str[i + 1] === '}' && stateStack.length === 0) {
        result.push('\\}');
        i += 1;
      } else if (str[i] === '{' && peek()?.inFormattedStr) {
        result.push(str[i]);
        stateStack.push({ inTemplate: true, depth: 1 });
      } else if (str[i] === '{' && peek()?.inTemplate) {
        result.push(str[i]);
        const curState = peek();
        curState.depth = (curState.depth || 0) + 1;
      } else if (str[i] === '}' && peek()?.inTemplate) {
        result.push(str[i]);
        const curState = peek();
        curState.depth = (curState.depth || 0) - 1;
        if (curState.depth === 0) {
          stateStack.pop();
        }
      } else {
        result.push(str[i]);
      }
    }
    return result.join('');
  }

  private parsePythonIntoArrowFunction(codeString: string, cubeName, originalObj, errorsReport: ErrorReporter) {
    const ast = this.parsePythonAndTranspileToJs(codeString, errorsReport);
    return this.astIntoArrowFunction(ast as any, codeString, cubeName);
  }

  private parsePythonAndTranspileToJs(codeString: string, errorsReport: ErrorReporter): t.Program | t.NullLiteral {
    if (codeString === '' || codeString === 'f""') {
      return t.nullLiteral();
    }

    try {
      const pythonParser = new PythonParser(codeString);
      return pythonParser.transpileToJs();
    } catch (e: any) {
      errorsReport.error(`Failed to parse Python expression. Most likely this type of syntax isn't supported yet: ${e.message || e}`);
    }

    return t.nullLiteral();
  }

  private astIntoArrowFunction(input: t.Program | t.NullLiteral, codeString: string, cubeName, resolveSymbol?: (string) => any) {
    const initialJs = babelGenerator(input, {}, codeString).code;

    // Re-parse generated JS to set all necessary parent paths
    const ast = parse(
      initialJs,
      {
        sourceType: 'script',
        plugins: ['objectRestSpread'],
      },
    );

    resolveSymbol = resolveSymbol || (n => this.viewCompiler.resolveSymbol(cubeName, n) ||
      this.cubeSymbols.resolveSymbol(cubeName, n) ||
      this.cubeSymbols.isCurrentCube(n));

    const traverseObj = {
      Program: (babelPath) => {
        CubePropContextTranspiler.replaceValueWithArrowFunction(<(string) => any>resolveSymbol, babelPath.get('body')[0].get('expression'));
      },
    };

    babelTraverse(ast, traverseObj);

    const body: any = ast.program.body[0];
    return body?.expression;
  }

  private checkDuplicateNames(items: any[], errorsReport: ErrorReporter, message: (name: string) => string) {
    const names = items
      .map(item => item?.name)
      .filter((name): name is string => name != null);

    const seen = new Set<string>();
    for (const name of names) {
      if (seen.has(name)) {
        errorsReport.error(message(name));
      }
      seen.add(name);
    }
  }

  private yamlArrayToObj(
    yamlArray,
    memberType: string,
    errorsReport: ErrorReporter,
    ctx: { cubeName: string; parent?: { type: string; name: string } }
  ) {
    if (!Array.isArray(yamlArray)) {
      errorsReport.error(`${memberType}s must be defined as array`);
      return {};
    }

    // Check for duplicate names
    this.checkDuplicateNames(yamlArray, errorsReport, (name) => {
      if (ctx.parent) {
        return `Found duplicate ${memberType} '${name}' in ${ctx.parent.type} '${ctx.parent.name}' in cube '${ctx.cubeName}'.`;
      }

      return `Member names must be unique within a cube. Found duplicate ${memberType} '${name}' in cube '${ctx.cubeName}'.`;
    });

    const remapped = yamlArray.map(({ name, indexes, granularities, timeShift, ...rest }) => {
      if (!name) {
        errorsReport.error(`name isn't defined for ${memberType}: ${JSON.stringify(rest)}`);
        return {};
      }

      const res = { [name]: {} };
      if (memberType === 'preAggregation' && indexes) {
        indexes = this.yamlArrayToObj(indexes || [], 'preAggregation.index', errorsReport, {
          cubeName: ctx.cubeName,
          parent: { type: 'pre-aggregation', name }
        });
        res[name] = { indexes, ...res[name] };
      }

      if (memberType === 'dimension' && granularities) {
        granularities = this.yamlArrayToObj(granularities || [], 'dimension.granularity', errorsReport, {
          cubeName: ctx.cubeName,
          parent: { type: 'time dimension', name }
        });
        res[name] = { granularities, ...res[name] };
      }

      if (timeShift) {
        this.checkDuplicateNames(
          timeShift,
          errorsReport,
          (shiftName) => `Time shift names must be unique within a ${memberType}. Found duplicate time shift '${shiftName}' in ${memberType} '${name}' in cube '${ctx.cubeName}'.`
        );

        res[name] = { timeShift, ...res[name] };
      }

      res[name] = { ...res[name], ...rest };

      return res;
    });

    return remapped.reduce((a, b) => ({ ...a, ...b }), {});
  }

  private extractProgramBodyIfNeeded(ast: t.Node) {
    if (t.isProgram(ast)) {
      const body: any = ast?.body[0];
      return body?.expression;
    }

    return ast;
  }
}
