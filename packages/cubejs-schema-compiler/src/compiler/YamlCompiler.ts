import YAML from 'js-yaml';
import * as t from '@babel/types';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';

import type { FileContent } from '@cubejs-backend/shared';

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

  public constructor(private cubeSymbols: CubeSymbols, private cubeDictionary: CubeDictionary) {
  }

  public compileYamlFile(file: FileContent, errorsReport: ErrorReporter, cubes, contexts, exports, asyncModules, toCompile, compiledFiles) {
    if (!file.content.trim()) {
      return;
    }

    const yamlObj = YAML.load(file.content);
    if (!yamlObj) {
      return;
    }

    for (const key of Object.keys(yamlObj)) {
      if (key === 'cubes') {
        (yamlObj.cubes || []).forEach(({ name, ...cube }) => {
          const transpiledFile = this.transpileAndPrepareJsFile(file, 'cube', { name, ...cube }, errorsReport);
          this.dataSchemaCompiler?.compileJsFile(transpiledFile, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles);
        });
      } else if (key === 'views') {
        (yamlObj.views || []).forEach(({ name, ...cube }) => {
          const transpiledFile = this.transpileAndPrepareJsFile(file, 'view', { name, ...cube }, errorsReport);
          this.dataSchemaCompiler?.compileJsFile(transpiledFile, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles);
        });
      } else {
        errorsReport.error(`Unexpected YAML key: ${key}. Only 'cubes' and 'views' are allowed here.`);
      }
    }
  }

  private transpileAndPrepareJsFile(file, methodFn, cubeObj, errorsReport: ErrorReporter) {
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
    cubeObj.preAggregations = this.yamlArrayToObj(cubeObj.preAggregations || [], 'segment', errorsReport);
    cubeObj.joins = this.yamlArrayToObj(cubeObj.joins || [], 'join', errorsReport);

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
              const ast = this.parsePythonAndTranspileToJs(code, errorsReport);
              return ast?.body[0]?.expression;
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
      return ast?.body[0]?.expression;
    } else if (typeof obj === 'boolean') {
      return t.booleanLiteral(obj);
    }
    if (typeof obj === 'object') {
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
      } else if (str[i] === '{' && str[i + 1] === '{' && peek()?.inFormattedStr) {
        result.push('{{');
        i += 1;
      } else if (str[i] === '}' && str[i + 1] === '}' && peek()?.inFormattedStr) {
        result.push('}}');
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

  private parsePythonIntoArrowFunction(codeString, cubeName, originalObj, errorsReport: ErrorReporter) {
    const ast = this.parsePythonAndTranspileToJs(codeString, errorsReport);
    return this.astIntoArrowFunction(ast, codeString, cubeName);
  }

  private parsePythonAndTranspileToJs(codeString, errorsReport: ErrorReporter) {
    try {
      const pythonParser = new PythonParser(codeString);
      return pythonParser.transpileToJs();
    } catch (e: any) {
      errorsReport.error(`Can't parse python expression. Most likely this type of syntax isn't supported yet: ${e.message || e}`);
    }
    return t.nullLiteral();
  }

  private astIntoArrowFunction(ast, codeString, cubeName, resolveSymbol?: (string) => any) {
    const initialJs = babelGenerator(ast, {}, codeString).code;

    // Re-parse generated JS to set all necessary parent paths
    ast = parse(
      initialJs,
      {
        sourceType: 'script',
        plugins: ['objectRestSpread'],
      },
    );

    resolveSymbol = resolveSymbol || (n => this.cubeSymbols.resolveSymbol(cubeName, n) || this.cubeSymbols.isCurrentCube(n));

    const traverseObj = {
      Program: (babelPath) => {
        CubePropContextTranspiler.replaceValueWithArrowFunction(<(string) => any>resolveSymbol, babelPath.get('body')[0].get('expression'));
      },
    };

    babelTraverse(ast, traverseObj);

    return ast.program.body[0]?.expression;
  }

  private yamlArrayToObj(yamlArray, memberType: string, errorsReport: ErrorReporter) {
    if (!Array.isArray(yamlArray)) {
      errorsReport.error(`${memberType}s must be defined as array`);
      return {};
    }

    const remapped = yamlArray.map(({ name, ...rest }) => {
      if (!name) {
        errorsReport.error(`name isn't defined for ${memberType}: ${YAML.stringify(rest)}`);
        return {};
      } else {
        return { [name]: rest };
      }
    });

    return remapped.reduce((a, b) => ({ ...a, ...b }), {});
  }
}
