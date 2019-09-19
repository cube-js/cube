import traverse from "@babel/traverse";
import fetch from './playgroundFetch';
import AppSnippet from './source/AppSnippet';
import TargetSource from './source/TargetSource';
import ChartSnippet from "./source/ChartSnippet";
import ScaffoldingSources from "./codegen/ScaffoldingSources";
import MergeScaffolding from "./source/MergeScaffolding";

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
      this.filesToPersist = [];
      this.parse(result.fileContents);
    }
  }

  async persistFiles(files) {
    return fetchWithRetry('/playground/dashboard-app-files', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        files
      })
    }, 5);
  }

  async persist() {
    const updateIndexCss = this.appLayoutAdded ? [
      { ...this.indexCssFile, content: this.indexCssFile.content + indexCss }
    ] : [];
    await this.persistFiles(this.filesToPersist.concat([
      { ...this.appFile, content: this.appTargetSource.formattedCode() }
    ]).concat(updateIndexCss));
    this.appLayoutAdded = false;
    const dependencies = this.appTargetSource.imports
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
    this.appTargetSource = new TargetSource(this.appFile.fileName, this.appFile.content);
  }

  ensureDashboardIsInApp() {
    let dashboardAdded = false;
    let headerElement = null;
    traverse(this.appTargetSource.ast, {
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
      const appSnippet = new AppSnippet();
      appSnippet.mergeTo(this.appTargetSource);
    }
    if (!this.sourceFiles.find(f => f.fileName === '/src/QueryBuilder/ExploreQueryBuilder.js')) {
      const queryBuilderFileNames = Object.keys(ScaffoldingSources)
        .filter(fileName => fileName.indexOf('react/QueryBuilder/') === 0)
        .map(MergeScaffolding.targetSourceName);
      this.filesToPersist = this.filesToPersist.concat(queryBuilderFileNames.map(f => ({
        fileName: f,
        content: new MergeScaffolding(
          f, this.sourceFiles.find(sourceFile => sourceFile.fileName === sourceFile)
        ).formattedMergeResult()
      })));
    }
  }

  async addChart(chartCode) {
    await this.load(true);
    if (this.loadError) {
      return;
    }
    this.ensureDashboardIsInApp();
    const chartSnippet = new ChartSnippet(chartCode);
    chartSnippet.mergeTo(this.appTargetSource);
    await this.persist();
  }

  dashboardAppCode() {
    return this.appTargetSource.code();
  }
}

export default DashboardSource;
