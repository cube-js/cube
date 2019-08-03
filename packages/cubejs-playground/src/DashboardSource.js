import { parse } from '@babel/parser';
import traverse from "@babel/traverse";
import generator from "@babel/generator";
import * as t from "@babel/types";
import fetch from './playgroundFetch';

const prettier = require("prettier/standalone");
// eslint-disable-next-line global-require
const plugins = [require("prettier/parser-babylon")];

const dashboardComponents = `
import { Row, Col, Card, Layout } from 'antd';
import 'antd/dist/antd.css';
import './index.css'

const AppLayout = ({ children }) => (
  <Layout>
    <Layout.Header>
      <div style={{ float: 'left' }}>
        <h2
          style={{
            color: "#fff",
            margin: 0,
            marginRight: '1em'
          }}
        >
          My Dashboard
        </h2>
      </div>
    </Layout.Header>
    <Layout.Content
      style={{
        padding: "0 25px 25px 25px",
        margin: "25px"
      }}
    >
      {children}
    </Layout.Content>
  </Layout>
)

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

const indexCss = `
body {
  background-color: #f0f2f5 !important;
}
`;

const fetchWithRetry = (url, options, retries) => fetch(url, { ...options, retries });

class DashboardSource {
  async load(createApp) {
    this.loadError = null;
    if (createApp) {
      await fetchWithRetry('/playground/ensure-dashboard-app', undefined, 5);
    }
    const res = await fetchWithRetry('/playground/dashboard-app-files', undefined, 5);
    const result = await res.json();
    if (result.error) {
      this.loadError = result.error;
    } else {
      this.sourceFiles = result.fileContents;
      this.parse(result.fileContents);
    }
  }

  async persist() {
    const updateIndexCss = this.appLayoutAdded ? [
      { ...this.indexCssFile, content: this.indexCssFile.content + indexCss }
    ] : [];
    await fetchWithRetry('/playground/dashboard-app-files', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        files: [
          { ...this.appFile, content: prettier.format(this.dashboardAppCode(), { parser: "babylon", plugins }) }
        ].concat(updateIndexCss)
      })
    }, 5);
    this.appLayoutAdded = false;
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
    if (!this.appFile) {
      throw new Error(`src/App.js file not found. Can't parse dashboard app. Please delete dashboard-app directory and try to create it again.`);
    }
    this.indexCssFile = sourceFiles.find(f => f.fileName.indexOf('src/index.css') !== -1);
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
        if (path.parent.type === 'Program') {
          this.definitions.push(...path.get('declarations'));
        }
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
    if (!this.appClass) {
      traverse(this.appAst, {
        FunctionDeclaration: (path) => {
          if (path.get('id').node.name === 'App') {
            this.appClass = path;
          }
        }
      });
    }
    if (!this.appClass) {
      throw new Error(`App class not found. Can't parse dashboard app.  Please delete dashboard-app directory and try to create it again.`);
    }
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
      this.appLayoutAdded = true;
      headerElement.parentPath.replaceWith(
        t.JSXElement(
          t.JSXOpeningElement(t.JSXIdentifier('AppLayout'), []),
          t.JSXClosingElement(t.JSXIdentifier('AppLayout')),
          [t.JSXElement(
            t.JSXOpeningElement(t.JSXIdentifier('Dashboard'), []),
            t.JSXClosingElement(t.JSXIdentifier('Dashboard')),
            []
          )]
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
    await this.load(true);
    if (this.loadError) {
      return;
    }
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
        if (path.parent.type === 'Program') {
          if (path.get('declarations')[0].get('id').get('name').node === 'ChartRenderer') {
            const chartRendererElement = path.get('declarations')[0].get('init').get('body');
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
      }
    });

    chartImports.forEach(i => this.mergeImport(i));
    definitions.forEach(d => this.mergeDefinition(d));
  }

  dashboardAppCode() {
    return this.appAst && generator(this.appAst, {}, this.appFile.content).code;
  }
}

export default DashboardSource;
