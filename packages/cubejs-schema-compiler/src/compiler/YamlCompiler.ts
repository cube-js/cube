import YAML from 'js-yaml';
import { camelize } from 'inflection';
import * as t from '@babel/types';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';
import { CubePropContextTranspiler, transpiledFields, transpiledFieldsPatterns } from './transpilers';
import { PythonParser } from '../parser/PythonParser';
import { CubeSymbols } from './CubeSymbols';
import { DataSchemaCompiler } from './DataSchemaCompiler';
import { nonStringFields } from './CubeValidator';

type EscapeStateStack = {
  inFormattedStr?: boolean;
  inStr?: boolean;
  inTemplate?: boolean;
  depth?: number;
};

export class YamlCompiler {
  public dataSchemaCompiler: DataSchemaCompiler | null = null;

  public constructor(private cubeSymbols: CubeSymbols) {
  }

  public compileYamlFile(file, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles) {
    const yamlObj = YAML.load(file.content);
    for (const key of Object.keys(yamlObj)) {
      if (key === 'cubes') {
        yamlObj.cubes.forEach(({ name, ...cube }) => {
          const transpiledFile = this.transpileAndPrepareJsFile(file, 'cube', { name, ...cube }, errorsReport);
          this.dataSchemaCompiler?.compileJsFile(transpiledFile, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles);
        });
      } else if (key === 'views') {
        yamlObj.views.forEach(({ name, ...cube }) => {
          const transpiledFile = this.transpileAndPrepareJsFile(file, 'view', { name, ...cube }, errorsReport);
          this.dataSchemaCompiler?.compileJsFile(transpiledFile, errorsReport, cubes, contexts, exports, asyncModules, toCompile, compiledFiles);
        });
      } else {
        errorsReport.error(`Unexpected YAML key: ${key}. Only 'cubes' and 'views' are allowed here.`);
      }
    }
  }

  private transpileAndPrepareJsFile(file, methodFn, cubeObj, errorsReport) {
    const yamlAst = this.transformYamlCubeObj(cubeObj, errorsReport);

    const cubeOrViewCall = t.callExpression(t.identifier(methodFn), [t.stringLiteral(cubeObj.name), yamlAst]);

    const content = babelGenerator(cubeOrViewCall, {}, '').code;
    return {
      fileName: file.fileName,
      content
    };
  }

  private transformYamlCubeObj(cubeObj, errorsReport) {
    cubeObj = this.camelizeObj(cubeObj);
    cubeObj.measures = this.yamlArrayToObj(cubeObj.measures || [], 'measure', errorsReport);
    cubeObj.dimensions = this.yamlArrayToObj(cubeObj.dimensions || [], 'dimension', errorsReport);
    cubeObj.segments = this.yamlArrayToObj(cubeObj.segments || [], 'segment', errorsReport);
    cubeObj.preAggregations = this.yamlArrayToObj(cubeObj.preAggregations || [], 'segment', errorsReport);
    cubeObj.joins = this.yamlArrayToObj(cubeObj.joins || [], 'join', errorsReport);
    return this.transpileYaml(cubeObj, [], cubeObj.name, errorsReport);
  }

  private camelizeObj(cubeObjPart: any): any {
    if (typeof cubeObjPart === 'object') {
      if (Array.isArray(cubeObjPart)) {
        for (let i = 0; i < cubeObjPart.length; i++) {
          cubeObjPart[i] = this.camelizeObj(cubeObjPart[i]);
        }
      } else {
        for (const key of Object.keys(cubeObjPart)) {
          cubeObjPart[key] = this.camelizeObj(cubeObjPart[key]);
          const camelizedKey = camelize(key, true);
          if (camelizedKey !== key) {
            cubeObjPart[camelizedKey] = cubeObjPart[key];
            delete cubeObjPart[key];
          }
        }
      }
    }
    return cubeObjPart;
  }

  private transpileYaml(obj, propertyPath, cubeName, errorsReport) {
    if (transpiledFields.has(propertyPath[propertyPath.length - 1])) {
      for (const p of transpiledFieldsPatterns) {
        const fullPath = propertyPath.join('.');
        if (fullPath.match(p)) {
          if (typeof obj === 'string' && propertyPath[propertyPath.length - 1] === 'sql') {
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

  private parsePythonIntoArrowFunction(codeString, cubeName, originalObj, errorsReport) {
    const ast = this.parsePythonAndTranspileToJs(codeString, errorsReport);
    return this.astIntoArrowFunction(ast, codeString, cubeName);
  }

  private parsePythonAndTranspileToJs(codeString, errorsReport) {
    try {
      const pythonParser = new PythonParser(codeString);
      return pythonParser.transpileToJs();
    } catch (e) {
      errorsReport.error(`Can't parse python expression. Most likely this type of syntax isn't supported yet: ${e.message || e}`);
    }
    return t.nullLiteral();
  }

  private astIntoArrowFunction(ast, codeString, cubeName) {
    const initialJs = babelGenerator(ast, {}, codeString).code;

    // Re-parse generated JS to set all necessary parent paths
    ast = parse(
      initialJs,
      {
        sourceType: 'script',
        plugins: ['objectRestSpread'],
      },
    );

    const resolveSymbol = n => this.cubeSymbols.resolveSymbol(cubeName, n) || this.cubeSymbols.isCurrentCube(n);

    const traverseObj = {
      Program: (babelPath) => {
        CubePropContextTranspiler.replaceValueWithArrowFunction(resolveSymbol, babelPath.get('body')[0].get('expression'));
      },
    };

    babelTraverse(ast, traverseObj);

    return ast.program.body[0]?.expression;
  }

  private yamlArrayToObj(yamlArray, memberType, errorsReport) {
    return yamlArray.map(({ name, ...rest }) => {
      if (!name) {
        errorsReport.error(`name isn't defined for ${memberType}: ${YAML.stringify(rest)}`);
        return {};
      } else {
        return { [name]: rest };
      }
    }).reduce((a, b) => ({ ...a, ...b }), {});
  }
}
