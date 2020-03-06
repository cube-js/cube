const { parse } = require("@babel/parser");
const traverse = require("@babel/traverse").default;
const generator = require("@babel/generator").default;

const prettier = require("prettier/standalone");
// eslint-disable-next-line global-require
const plugins = [require("prettier/parser-babylon")];

class TargetSource {
  constructor(fileName, source) {
    this.source = source;
    this.fileName = fileName;
    try {
      this.ast = parse(source, {
        sourceFilename: fileName,
        sourceType: 'module',
        plugins: [
          "jsx"
        ]
      });
    } catch (e) {
      throw new Error(`Can't parse ${fileName}: ${e.message}`);
    }
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
      },
      FunctionDeclaration: (path) => {
        if (path.parent.type === 'Program') {
          this.definitions.push(path);
        }
      }
    });
  }

  code() {
    return this.ast && generator(this.ast, {}, this.source).code;
  }

  formattedCode() {
    return TargetSource.formatCode(this.code());
  }

  static formatCode(code) {
    return prettier.format(code, { parser: "babylon", plugins });
  }
}

module.exports = TargetSource;
