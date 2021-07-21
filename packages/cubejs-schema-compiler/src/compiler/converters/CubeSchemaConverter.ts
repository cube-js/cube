import generator from '@babel/generator';
import { parse } from '@babel/parser';
import traverse from '@babel/traverse';
import * as t from '@babel/types';

export type AstSet = {
  fileName: string;
  ast: t.File;
  cubeDefinition: t.ObjectExpression;
};

export type AstByCubeName = Record<string, AstSet>;

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

  protected async prepare(): Promise<void> {
    this.dataSchemaFiles = await this.fileRepository.dataSchemaFiles();

    this.dataSchemaFiles.forEach((schemaFile) => {
      const ast = this.parse(schemaFile);

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

                if (t.isObjectExpression(args[1]?.node) && ast != null) {
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
    });
  }

  protected parse(file: SchemaFile) {
    try {
      return parse(file.content, {
        sourceFilename: file.fileName,
        sourceType: 'module',
        plugins: ['objectRestSpread'],
      });
    } catch (error) {
      if (error.toString().indexOf('SyntaxError') !== -1) {
        const line = file.content.split('\n')[error.loc.line - 1];
        const spaces = Array(error.loc.column).fill(' ').join('');

        throw new Error(`Syntax error during '${file.fileName}' parsing: ${error.message}:\n${line}\n${spaces}^`);
      }

      throw error;
    }
  }

  public async generate() {
    await this.prepare();

    this.converters.forEach((converter) => {
      converter.convert(this.parsedFiles);
    });
  }

  public getSourceFiles() {
    return Object.entries(this.parsedFiles).map(([cubeName, file]) => ({
      cubeName,
      fileName: file.fileName,
      source: generator(file.ast, {}).code,
    }));
  }
}
