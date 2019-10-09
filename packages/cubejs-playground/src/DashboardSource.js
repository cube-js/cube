/* globals window */
import traverse from "@babel/traverse";
import fetch from './playgroundFetch';
import AppSnippet from './source/AppSnippet';
import TargetSource from './source/TargetSource';
import ScaffoldingSources from "./codegen/ScaffoldingSources";
import MergeScaffolding from "./source/MergeScaffolding";
import IndexSnippet from "./source/IndexSnippet";
import ExploreSnippet from "./source/ExploreSnippet";
import ChartRendererSnippet from "./source/ChartRendererSnippet";
import DashboardStoreSnippet from "./source/DashboardStoreSnippet";
import SourceSnippet from "./source/SourceSnippet";

const indexCss = `
@import '~antd/dist/antd.css';
body {
  background-color: #f0f2f5 !important;
}
`;

const fetchWithRetry = (url, options, retries) => fetch(url, { ...options, retries });

class DashboardSource {
  async load(createApp, { chartLibrary }) {
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
    if (!result.error && this.ensureDashboardIsInApp({ chartLibrary })) {
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
      { ...this.indexCssFile, content: indexCss }
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
        return this.withPeerDependencies(dependency);
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

  // TODO move to dev server
  withPeerDependencies(dependency) {
    let result = {
      [dependency]: 'latest'
    };
    if (dependency === 'graphql-tag') {
      result = {
        ...result,
        graphql: 'latest'
      };
    }
    if (dependency === 'react-chartjs-2') {
      result = {
        ...result,
        'chart.js': 'latest'
      };
    }
    return result;
  }

  parse(sourceFiles) {
    this.appFile = sourceFiles.find(f => f.fileName.indexOf('src/App.js') !== -1);
    if (!this.appFile) {
      throw new Error(`src/App.js file not found. Can't parse dashboard app. Please delete dashboard-app directory and try to create it again.`);
    }
    this.indexCssFile = sourceFiles.find(f => f.fileName.indexOf('src/index.css') !== -1);
    this.appTargetSource = this.targetSourceByFile('/src/App.js');
  }

  ensureDashboardIsInApp({ chartLibrary }) {
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
      const scaffoldingFileToSnippet = {
        'react/App.js': new AppSnippet(),
        'react/index.js': new IndexSnippet(this.playgroundContext),
        'react/pages/ExplorePage.js': new ExploreSnippet(),
        'react/components/ChartRenderer.js': new ChartRendererSnippet(chartLibrary)
      };

      const scaffoldingFileNames = Object.keys(ScaffoldingSources)
        .filter(fileName => fileName.indexOf('react/') === 0);

      scaffoldingFileNames.forEach(scaffoldingFile => {
        this.mergeSnippetToFile(
          scaffoldingFileToSnippet[scaffoldingFile] || new SourceSnippet(ScaffoldingSources[scaffoldingFile]),
          MergeScaffolding.targetSourceName(scaffoldingFile)
        );
      });
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

  /*
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
  */

  dashboardAppCode() {
    return this.appTargetSource.code();
  }
}

export default DashboardSource;
