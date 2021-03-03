const fs = require('fs-extra');
const {
  SourceSnippet,
  TargetSource,
  utils,
} = require('@cubejs-templates/core');
const { pascalCase } = require('change-case');

const DependencTree = require('../dev/DependencyTree');
const AppContainer = require('../dev/AppContainer');
const DevPackageFetcher = require('../dev/DevPackageFetcher');
const path = require('path');
const { executeCommand } = require('../dev/utils');
const { join } = require('path');
const { generateCodeChunks } = require('./code-chunks-gen');

const repo = {
  owner: 'cube-js',
  name: 'cubejs-playground-templates',
};

const chartingLibraryTemplates = [
  'recharts-charts',
  'bizcharts-charts',
  'd3-charts',
  'chartjs-charts',
];

const packages = [
  // 'dev-cra',
  'create-react-app',
  'react-charts',
];

(async () => {
  const fetcher = new DevPackageFetcher(repo);
  const manifest = await fetcher.manifestJSON();
  const { packagesPath } = await fetcher.downloadPackages();

  const rootPath = path.resolve(`${__dirname}/../..`);

  const distPath = `${rootPath}/charts-dist/react`;
  const reactChartsPath = `${distPath}/react-charts`;

  let dependencies = [['chart.js', '2.9.4']];

  chartingLibraryTemplates.forEach(async (key) => {
    const dashboardAppPath = `${distPath}/${key}`;
    const dt = new DependencTree(manifest, [
      key,
      'react-charting-library',
      'antd-tables',
    ]);

    const appContainer = new AppContainer(
      dt.getRootNode(),
      {
        appPath: dashboardAppPath,
        packagesPath,
      },
      {}
    );

    await appContainer.applyTemplates();
    dependencies = dependencies.concat(
      Object.entries(appContainer.sourceContainer.importDependencies)
    );
  });

  const dt = new DependencTree(manifest, packages);

  const appContainer = new AppContainer(dt.getRootNode(), {
    appPath: reactChartsPath,
    packagesPath,
  });

  await appContainer.applyTemplates();

  let code = '';
  const imports = [];
  const libNames = [];

  try {
    await Promise.all(
      chartingLibraryTemplates.map(async (key) => {
        const fileContents = await utils.fileContentsRecursive(
          `${distPath}/${key}`
        );

        const chartRendererContent = fileContents.find(
          ({ fileName }) => fileName === '/src/components/ChartRenderer.js'
        );

        const codeChunksContent = generateCodeChunks(
          chartRendererContent.content
        );

        await executeCommand('mv', [
          `${distPath}/${key}`,
          `${reactChartsPath}/src`,
        ]);
        fs.writeFileSync(
          `${reactChartsPath}/src/${key}/src/code-chunks.js`,
          codeChunksContent
        );

        libNames.push(`${key.replace(/-([^-]+)/, '')}: ${pascalCase(key)}`);
        imports.push(
          `import ${pascalCase(
            key
          )} from './${key}/src/components/ChartRenderer';`
        );
      })
    );
  } catch (error) {
    console.log(error);
  }

  code += imports.join('\n');
  code += `
    const libs = {
      ${libNames.join(',\n')}
    };
  `;

  const libsSnippet = new SourceSnippet(code);
  const appTarget = new TargetSource(
    'app.js',
    fs.readFileSync(join(reactChartsPath, 'src/App.js'), 'utf-8')
  );

  libsSnippet.mergeTo(appTarget);
  fs.writeFileSync(join(reactChartsPath, 'src/App.js'), appTarget.code());

  appContainer.sourceContainer.addImportDependencies(
    dependencies
      .map(([d, v]) => ({ [d]: v }))
      .reduce((a, b) => ({ ...a, ...b }))
  );
  await appContainer.ensureDependencies();
})();

// npm install --save antd @ant-design/compatible @cubejs-client/core @cubejs-client/react chart.js bizcharts recharts d3 react-chartjs
