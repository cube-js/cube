import generator from '@babel/generator';
import { parse } from '@babel/parser';
import traverse from '@babel/traverse';
import * as t from '@babel/types';
import YAML, { isMap, isSeq } from 'yaml';

export type JsSet = {
  fileName: string;
  ast: t.File;
  cubeDefinition: t.ObjectExpression;
};

export type YamlSet = {
  fileName: string;
  yaml: YAML.Document;
  cubeDefinition: YAML.YAMLMap;
};

const JINJA_SYNTAX = /{%|%}|{{|}}/ig;

export type AstByCubeName = Record<string, (JsSet | YamlSet)>;

export interface CubeConverterInterface {
  convert(astByCubeName: AstByCubeName): void;
}

type SchemaFile = {
  fileName: string;
  content: string;
};

export class CubeSchemaConverter {
  protected dataSchemaFiles: SchemaFile[] = [];

  protected parsedFiles: AstByCubeName = {};

  public constructor(protected fileRepository: any, protected converters: CubeConverterInterface[]) {}

  /**
   * Parse Schema files from the repository and create a mapping of cube names to schema files.
   * If optional `cubeName` parameter is passed - only file with asked cube is parsed and stored.
   * @param cubeName
   * @protected
   */
  protected async prepare(cubeName?: string): Promise<void> {
    this.dataSchemaFiles = await this.fileRepository.dataSchemaFiles();

    this.dataSchemaFiles.forEach((schemaFile) => {
      if (schemaFile.fileName.endsWith('.js')) {
        this.transformJS(schemaFile, cubeName);
      } else if ((schemaFile.fileName.endsWith('.yml') || schemaFile.fileName.endsWith('.yaml')) && !schemaFile.content.match(JINJA_SYNTAX)) {
        // Jinja-templated data models are not supported in Rollup Designer yet, so we're ignoring them,
        // and if user has chosen the cube from such file - it won't be found during generation.
        this.transformYaml(schemaFile, cubeName);
      }
    });
  }

  protected transformYaml(schemaFile: SchemaFile, filterCubeName?: string) {
    if (!schemaFile.content.trim()) {
      return;
    }

    const yamlDoc = YAML.parseDocument(schemaFile.content);
    if (!yamlDoc?.contents) {
      return;
    }

    const root = yamlDoc.contents;

    if (!isMap(root)) {
      return;
    }

    const cubesPair = root.items.find((item) => {
      const key = item.key as YAML.Scalar;
      return key?.value === 'cubes';
    });

    if (!cubesPair || !isSeq(cubesPair.value)) {
      return;
    }

    for (const cubeNode of cubesPair.value.items) {
      if (isMap(cubeNode)) {
        const cubeNamePair = cubeNode.items.find((item) => {
          const key = item.key as YAML.Scalar;
          return key?.value === 'name';
        });

        const cubeName = (cubeNamePair?.value as YAML.Scalar).value;

        if (cubeName && typeof cubeName === 'string' && (!filterCubeName || cubeName === filterCubeName)) {
          this.parsedFiles[cubeName] = {
            fileName: schemaFile.fileName,
            yaml: yamlDoc,
            cubeDefinition: cubeNode,
          };

          if (cubeName === filterCubeName) {
            return;
          }
        }
      }
    }
  }

  protected transformJS(schemaFile: SchemaFile, filterCubeName?: string) {
    const ast = this.parseJS(schemaFile);

    traverse(ast, {
      CallExpression: (path) => {
        if (t.isIdentifier(path.node.callee)) {
          const args = path.get('arguments');

          if (path.node.callee.name === 'cube') {
            if (args?.[args.length - 1]) {
              let cubeName: string | undefined;

              if (args[0].node.type === 'StringLiteral' && args[0].node.value) {
                cubeName = args[0].node.value;
              } else if (args[0].node.type === 'TemplateLiteral' && args[0].node.quasis?.[0]?.value.cooked) {
                cubeName = args[0].node.quasis?.[0]?.value.cooked;
              }

              if (cubeName == null) {
                throw new Error(`Error parsing ${schemaFile.fileName}`);
              }

              if (t.isObjectExpression(args[1]?.node) && ast != null && (!filterCubeName || cubeName === filterCubeName)) {
                this.parsedFiles[cubeName] = {
                  fileName: schemaFile.fileName,
                  ast,
                  cubeDefinition: args[1].node,
                };
              }
            }
          }
        }
      },
    });
  }

  protected parseJS(file: SchemaFile) {
    try {
      return parse(file.content, {
        sourceFilename: file.fileName,
        sourceType: 'module',
        plugins: ['objectRestSpread'],
      });
    } catch (error: any) {
      if (error.toString().indexOf('SyntaxError') !== -1) {
        const line = file.content.split('\n')[error.loc.line - 1];
        const spaces = Array(error.loc.column).fill(' ').join('');

        throw new Error(`Syntax error during '${file.fileName}' parsing: ${error.message}:\n${line}\n${spaces}^`);
      }

      throw error;
    }
  }

  public async generate(cubeName?: string) {
    await this.prepare(cubeName);

    this.converters.forEach((converter) => {
      converter.convert(this.parsedFiles);
    });
  }

  public getSourceFiles() {
    return Object.entries(this.parsedFiles).map(([cubeName, file]) => {
      const source = 'ast' in file
        ? generator(file.ast, {}).code
        : String(file.yaml);

      return {
        cubeName,
        fileName: file.fileName,
        source,
      };
    });
  }
}
