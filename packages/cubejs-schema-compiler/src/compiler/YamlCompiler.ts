import YAML from 'js-yaml';
import * as t from '@babel/types';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';
import { JinjaEngine, NativeInstance, PythonCtx } from '@cubejs-backend/native';

import type { FileContent } from '@cubejs-backend/shared';

import { getEnv } from '@cubejs-backend/shared';
import { CubePropContextTranspiler, transpiledFields, transpiledFieldsPatterns } from './transpilers';
import { PythonParser } from '../parser/PythonParser';
import { CubeSymbols } from './CubeSymbols';
import { DataSchemaCompiler } from './DataSchemaCompiler';
import { nonStringFields } from './CubeValidator';
import { CubeDictionary } from './CubeDictionary';
import { ErrorReporter } from './ErrorReporter';
import { camelizeCube } from './utils';

type EscapeStateStack = {
  inFormattedStr?: boolean;
  inStr?: boolean;
  inTemplate?: boolean;
  depth?: number;
};

export class YamlCompiler {
  public dataSchemaCompiler: DataSchemaCompiler | null = null;

  protected jinjaEngine: JinjaEngine | null = null;

  public constructor(
    private readonly cubeSymbols: CubeSymbols,
    private readonly cubeDictionary: CubeDictionary,
    private readonly nativeInstance: NativeInstance,
    private readonly viewCompiler: CubeSymbols,
  ) {
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

  public async renderTemplate(file: FileContent, compileContext, pythonContext: PythonCtx): Promise<FileContent> {
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
    compileContext,
    pythonContext: PythonCtx
  ) {
    const compiledFile = await this.renderTemplate(file, compileContext, pythonContext);

    return this.compileYamlFile(compiledFile, errorsReport);
  }

  public compileYamlFile(
    file: FileContent,
    errorsReport: ErrorReporter,
  ) {
    if (!file.content.trim()) {
      return;
    }

    const yamlObj: any = YAML.load(file.content);
    if (!yamlObj) {
      return;
    }

    for (const key of Object.keys(yamlObj)) {
      if (key === 'cubes') {
        (yamlObj.cubes || []).forEach(({ name, ...cube }) => {
          const transpiledFile = this.transpileAndPrepareJsFile(file, 'cube', { name, ...cube }, errorsReport);
          this.dataSchemaCompiler?.compileJsFile(transpiledFile, errorsReport);
        });
      } else if (key === 'views') {
        (yamlObj.views || []).forEach(({ name, ...cube }) => {
          const transpiledFile = this.transpileAndPrepareJsFile(file, 'view', { name, ...cube }, errorsReport);
          this.dataSchemaCompiler?.compileJsFile(transpiledFile, errorsReport);
        });
      } else {
        errorsReport.error(`Unexpected YAML key: ${key}. Only 'cubes' and 'views' are allowed here.`);
      }
    }
  }

  private transpileAndPrepareJsFile(file: FileContent, methodFn: ('cube' | 'view'), cubeObj, errorsReport: ErrorReporter): FileContent {
    const yamlAst = this.transformYamlCubeObj(cubeObj, errorsReport);

    const cubeOrViewCall = t.callExpression(t.identifier(methodFn), [t.stringLiteral(cubeObj.name), yamlAst]);

    const content = babelGenerator(cubeOrViewCall, {}, '').code;
    return {
      fileName: file.fileName,
      content
    };
  }

  private transformYamlCubeObj(cubeObj, errorsReport: ErrorReporter) {
    camelizeCube(cubeObj);

    cubeObj.measures = this.yamlArrayToObj(cubeObj.measures || [], 'measure', errorsReport);
    cubeObj.dimensions = this.yamlArrayToObj(cubeObj.dimensions || [], 'dimension', errorsReport);
    cubeObj.segments = this.yamlArrayToObj(cubeObj.segments || [], 'segment', errorsReport);
    cubeObj.preAggregations = this.yamlArrayToObj(cubeObj.preAggregations || [], 'preAggregation', errorsReport);

    cubeObj.joins = cubeObj.joins || []; // For edge cases where joins are not defined/null
    if (!Array.isArray(cubeObj.joins)) {
      errorsReport.error('joins must be defined as array');
      cubeObj.joins = [];
    }

    cubeObj.hierarchies = this.yamlArrayToObj(cubeObj.hierarchies || [], 'hierarchies', errorsReport);

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
      errorsReport.error(`Can't parse python expression. Most likely this type of syntax isn't supported yet: ${e.message || e}`);
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

  private yamlArrayToObj(yamlArray, memberType: string, errorsReport: ErrorReporter) {
    if (!Array.isArray(yamlArray)) {
      errorsReport.error(`${memberType}s must be defined as array`);
      return {};
    }

    const remapped = yamlArray.map(({ name, indexes, granularities, ...rest }) => {
      if (!name) {
        errorsReport.error(`name isn't defined for ${memberType}: ${JSON.stringify(rest)}`);
        return {};
      }

      const res = { [name]: {} };
      if (memberType === 'preAggregation' && indexes) {
        indexes = this.yamlArrayToObj(indexes || [], `${memberType}.index`, errorsReport);
        res[name] = { indexes, ...res[name] };
      }

      if (memberType === 'dimension' && granularities) {
        granularities = this.yamlArrayToObj(granularities || [], `${memberType}.granularity`, errorsReport);
        res[name] = { granularities, ...res[name] };
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
