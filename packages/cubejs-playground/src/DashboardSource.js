import { parse } from '@babel/parser';
import traverse from "@babel/traverse";
import generator from "@babel/generator";
import * as t from "@babel/types";
import { fetch } from 'whatwg-fetch';

const prettier = require("prettier/standalone");
// eslint-disable-next-line global-require
const plugins = [require("prettier/parser-babylon")];

const dashboardComponents = `
import { Row, Col, Card } from 'antd';
import 'antd/dist/antd.css';

const Dashboard = ({ children }) => (
  <Row type="flex" justify="space-around" align="top" gutter={24}>{children}</Row>
)

const DashboardItem = ({ children, title }) => (
  <Col span={24} lg={12}>
    <Card title={title} style={{ marginBottom: '24px' }}>
      {children}
    </Card>
  </Col>
)
`;

const fetchWithRetry = (url, options, retries) => fetch(url, options).catch(e => {
  if (e.message === 'Network request failed') {
    return retries > 0 ? fetchWithRetry(url, options, retries - 1) : fetch(url, options);
  }
  throw e;
});

class DashboardSource {
  async load() {
    const res = await fetchWithRetry('/playground/dashboard-app-files', undefined, 5);
    const result = await res.json();
    this.sourceFiles = result.fileContents;
    this.parse(result.fileContents);
  }

  async persist() {
    await fetchWithRetry('/playground/dashboard-app-files', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        files: [{ ...this.appFile, content: prettier.format(this.dashboardAppCode(), { parser: "babylon", plugins }) }]
      })
    }, 5);
    const dependencies = this.imports
      .filter(i => i.get('source').node.value.indexOf('.') !== 0)
      .map(i => {
        const importName = i.get('source').node.value.split('/');
        const dependency = importName[0].indexOf('@') === 0 ? [importName[0], importName[1]].join('/') : importName[0];
        return { [dependency]: 'latest' };
      }).reduce((a, b) => ({ ...a, ...b }));
    await fetchWithRetry('/playground/ensure-dependencies', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        dependencies
      })
    }, 5);
  }

  parse(sourceFiles) {
    this.appFile = sourceFiles.find(f => f.fileName.indexOf('src/App.js') !== -1);
    this.appAst = parse(this.appFile.content, {
      sourceFilename: this.appFile.fileName,
      sourceType: 'module',
      plugins: [
        "jsx"
      ]
    });
    this.findAllImports();
    this.findAllDefinitions();
    this.findAppClass();
  }

  findAllImports() {
    this.imports = [];
    traverse(this.appAst, {
      ImportDeclaration: (path) => {
        this.imports.push(path);
      }
    });
  }

  findAllDefinitions() {
    this.definitions = [];
    traverse(this.appAst, {
      VariableDeclaration: (path) => {
        this.definitions.push(...path.get('declarations'));
      }
    });
  }

  findAppClass() {
    traverse(this.appAst, {
      Class: (path) => {
        if (path.get('id').node.name === 'App') {
          this.appClass = path;
        }
      }
    });
  }

  ensureDashboardIsInApp() {
    let dashboardAdded = false;
    let headerElement = null;
    traverse(this.appAst, {
      JSXOpeningElement: (path) => {
        if (path.get('name').get('name').node === 'Dashboard') {
          dashboardAdded = true;
        }
        if (path.get('name').get('name').node === 'header'
          && path.get('attributes').find(
            a => a.get('name').get('name').node === 'className'
              && a.get('value').node
              && a.get('value').node.type === 'StringLiteral'
              && a.get('value').node.value === 'App-header'
          )
        ) {
          headerElement = path;
        }
      }
    });
    if (!dashboardAdded && headerElement) {
      headerElement.parentPath.replaceWith(
        t.JSXElement(
          t.JSXOpeningElement(t.JSXIdentifier('Dashboard'), []),
          t.JSXClosingElement(t.JSXIdentifier('Dashboard')),
          []
        )
      );
    }
    traverse(this.appAst, {
      JSXOpeningElement: (path) => {
        if (path.get('name').get('name').node === 'Dashboard') {
          this.dashboardElement = path;
        }
      }
    });
  }

  mergeImport(importDeclaration) {
    const sameSourceImport = this.imports.find(
      i => i.get('source').node.value === importDeclaration.get('source').node.value
        && (
          i.get('specifiers')[0] && i.get('specifiers')[0].type
        ) === (
          importDeclaration.get('specifiers')[0] && importDeclaration.get('specifiers')[0].type
        )
    );
    if (!sameSourceImport) {
      this.imports[this.imports.length - 1].insertAfter(importDeclaration.node);
      this.findAllImports();
    } else {
      importDeclaration.get('specifiers').forEach(toInsert => {
        const foundSpecifier = sameSourceImport.get('specifiers')
          .find(
            existing => (
              existing.get('imported').node && existing.get('imported').node.name
            ) === (
              toInsert.get('imported').node && toInsert.get('imported').node.name
            ) && (
              existing.get('local').node && existing.get('local').node.name
            ) === (
              toInsert.get('local').node && toInsert.get('local').node.name
            )
          );
        if (!foundSpecifier) {
          sameSourceImport.pushContainer('specifiers', toInsert.node);
        }
      });
    }
  }

  mergeDefinition(constDef) {
    constDef.get('declarations').forEach(declaration => {
      const existingDefinition = this.definitions.find(
        d => d.get('id').node.type === 'Identifier'
          && declaration.get('id').node.type === 'Identifier'
          && declaration.get('id').node.name === d.get('id').node.name
      );
      if (!existingDefinition) {
        this.appClass.insertBefore(t.variableDeclaration('const', [declaration.node]));
      }
    });
  }

  async addChart(chartCode) {
    await this.load();
    this.ensureDashboardIsInApp();
    this.performAddition(dashboardComponents);
    this.performAddition(chartCode);
    await this.persist();
  }

  performAddition(chartCode) {
    const chartAst = parse(chartCode, {
      sourceType: 'module',
      plugins: [
        "jsx"
      ]
    });

    const chartImports = [];

    traverse(chartAst, {
      ImportDeclaration: (path) => {
        chartImports.push(path);
      }
    });

    const definitions = [];

    traverse(chartAst, {
      VariableDeclaration: (path) => {
        if (path.get('declarations')[0].get('id').get('name').node === 'ChartRenderer') {
          const chartRendererElement = path.get('declarations')[0].get('init').get('body');
          console.log(path.get('declarations')[0].get('init').get('body'));
          this.dashboardElement.parentPath.pushContainer(
            'children',
            t.JSXElement(
              t.JSXOpeningElement(t.JSXIdentifier('DashboardItem'), []),
              t.JSXClosingElement(t.JSXIdentifier('DashboardItem')),
              [chartRendererElement.node]
            )
          );
        } else {
          definitions.push(path);
        }
      }
    });

    chartImports.forEach(i => this.mergeImport(i));
    definitions.forEach(d => this.mergeDefinition(d));
  }

  dashboardAppCode() {
    return generator(this.appAst, {}, this.appFile.content).code;
  }
}

export default DashboardSource;
