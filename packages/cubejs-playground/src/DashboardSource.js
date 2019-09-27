/* globals window */
import traverse from "@babel/traverse";
import fetch from './playgroundFetch';
import AppSnippet from './source/AppSnippet';
import TargetSource from './source/TargetSource';
import ChartSnippet from "./source/ChartSnippet";
import ScaffoldingSources from "./codegen/ScaffoldingSources";
import MergeScaffolding from "./source/MergeScaffolding";
import IndexSnippet from "./source/IndexSnippet";
import ExploreSnippet from "./source/ExploreSnippet";
import ChartRendererSnippet from "./source/ChartRendererSnippet";
import DashboardStoreSnippet from "./source/DashboardStoreSnippet";
import SourceSnippet from "./source/SourceSnippet";

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
    this.playgroundContext = await this.loadContext();
    this.fileToTargetSource = {};
    if (result.error) {
      this.loadError = result.error;
    } else {
      this.sourceFiles = result.fileContents;
      this.filesToPersist = [];
      this.parse(result.fileContents);
    }
    if (!result.error && this.ensureDashboardIsInApp()) {
      await this.persist();
    }
  }

  async loadContext() {
    const res = await fetch('/playground/context');
    const result = await res.json();
    return {
      cubejsToken: result.cubejsToken,
      apiUrl: result.apiUrl || window.location.href.split('#')[0].replace(/\/$/, '')
    };
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
    const toPersist = this.filesToPersist.concat(
      Object.keys(this.fileToTargetSource).map(fileName => ({
        fileName, content: this.fileToTargetSource[fileName].formattedCode()
      }))
    ).concat(updateIndexCss);
    await this.persistFiles(toPersist);
    this.appLayoutAdded = false;
    const allImports = toPersist
      .filter(f => f.fileName.match(/\.js$/))
      .map(f => new TargetSource(f.fileName, f.content).imports)
      .reduce((a, b) => a.concat(b));
    const dependencies = allImports
      .filter(i => i.get('source').node.value.indexOf('.') !== 0)
      .map(i => {
        const importName = i.get('source').node.value.split('/');
        const dependency = importName[0].indexOf('@') === 0 ? [importName[0], importName[1]].join('/') : importName[0];
        if (dependency === 'graphql-tag') {
          return {
            graphql: 'latest',
            [dependency]: 'latest'
          };
        }
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
    this.appTargetSource = this.targetSourceByFile('/src/App.js');
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
    let merged = false;
    if (!dashboardAdded && headerElement) {
      this.appLayoutAdded = true;
      const appSnippet = new AppSnippet();
      appSnippet.mergeTo(this.appTargetSource);
      this.mergeSnippetToFile(new IndexSnippet(this.playgroundContext), '/src/index.js');
      this.mergeSnippetToFile(new ExploreSnippet(), '/src/ExplorePage.js');
      this.mergeSnippetToFile(new ChartRendererSnippet(), '/src/ChartRenderer.js');
      this.mergeSnippetToFile(new DashboardStoreSnippet(), '/src/DashboardStore.js');
      this.mergeSnippetToFile(new SourceSnippet(ScaffoldingSources['react/DashboardPage.js']), '/src/DashboardPage.js');
      merged = true;
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
      merged = true;
    }
    return merged;
  }

  targetSourceByFile(fileName) {
    let file = this.sourceFiles.find(f => f.fileName === fileName);
    if (!file) {
      file = { fileName, content: ScaffoldingSources[MergeScaffolding.scaffoldingSourceName(fileName)] };
    }
    if (!this.fileToTargetSource[fileName]) {
      this.fileToTargetSource[fileName] = new TargetSource(file.fileName, file.content);
    }
    return this.fileToTargetSource[fileName];
  }

  mergeSnippetToFile(snippet, fileName) {
    const targetSource = this.targetSourceByFile(fileName);
    snippet.mergeTo(targetSource);
  }

  async addChart(chartCode) {
    await this.load(true);
    if (this.loadError) {
      return;
    }
    this.ensureDashboardIsInApp();
    const chartSnippet = new ChartSnippet(chartCode);
    this.mergeSnippetToFile(chartSnippet, '/src/DashboardPage.js');
    await this.persist();
  }

  dashboardAppCode() {
    return this.appTargetSource.code();
  }
}

export default DashboardSource;
