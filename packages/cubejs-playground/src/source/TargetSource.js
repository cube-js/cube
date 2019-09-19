import { parse } from '@babel/parser';
import traverse from "@babel/traverse";
import generator from "@babel/generator";

const prettier = require("prettier/standalone");
// eslint-disable-next-line global-require
const plugins = [require("prettier/parser-babylon")];

class TargetSource {
  constructor(fileName, source) {
    this.source = source;
    this.fileName = fileName;
    this.ast = parse(source, {
      sourceFilename: fileName,
      sourceType: 'module',
      plugins: [
        "jsx"
      ]
    });
    this.findAllImports();
    this.findAllDefinitions();
    this.findDefaultExport();
  }

  findAllImports() {
    this.imports = [];
    traverse(this.ast, {
      ImportDeclaration: (path) => {
        this.imports.push(path);
      }
    });
  }

  findDefaultExport() {
    traverse(this.ast, {
      ExportDefaultDeclaration: (path) => {
        if (path) {
          this.defaultExport = path;
        }
      }
    });
  }

  findAllDefinitions() {
    this.definitions = [];
    traverse(this.ast, {
      VariableDeclaration: (path) => {
        if (path.parent.type === 'Program') {
          this.definitions.push(...path.get('declarations'));
        }
      }
    });
  }

  code() {
    return this.ast && generator(this.ast, {}, this.source).code;
  }

  formattedCode() {
    return prettier.format(this.code(), { parser: "babylon", plugins });
  }
}

export default TargetSource;
