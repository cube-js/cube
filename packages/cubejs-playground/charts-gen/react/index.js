const fs = require('fs-extra');
const {
  SourceSnippet,
  TargetSource,
  utils,
} = require('@cubejs-templates/core');
const { pascalCase } = require('change-case');
const path = require('path');
const {
  DependencyTree,
  AppContainer,
  DevPackageFetcher,
  executeCommand
} = require('@cubejs-backend/templates');

const { generateCodeChunks } = require('./code-chunks-gen');
const { REPOSITORY } = require('../env');

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
  const fetcher = new DevPackageFetcher(REPOSITORY);
  const manifest = await fetcher.manifestJSON();
  const { packagesPath } = await fetcher.downloadPackages();

  const rootPath = path.resolve(`${__dirname}/../..`);

  const distPath = `${rootPath}/charts-dist/react`;
  const reactChartsPath = `${distPath}/react-charts`;

  let dependencies = [['chart.js', '2.9.4']];

  await Promise.all(
    chartingLibraryTemplates.map(async (key) => {
      const dashboardAppPath = `${distPath}/${key}`;
      const dt = new DependencyTree(manifest, [
        'react-charting-library',
        'antd-tables',
        key,
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
    })
  );

  const dt = new DependencyTree(manifest, packages);

  const appContainer = new AppContainer(dt.getRootNode(), {
    appPath: reactChartsPath,
    packagesPath,
  });

  await appContainer.applyTemplates();

  let code = '';
  const imports = [];
  const libNames = [];

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

  code += imports.join('\n');
  code += `
    const libs = {
      ${libNames.join(',\n')}
    };
  `;

  const libsSnippet = new SourceSnippet(code);
  const appTarget = new TargetSource(
    'app.js',
    fs.readFileSync(path.join(reactChartsPath, 'src/App.js'), 'utf-8')
  );

  libsSnippet.mergeTo(appTarget);
  fs.writeFileSync(path.join(reactChartsPath, 'src/App.js'), appTarget.code());

  appContainer.sourceContainer.addImportDependencies(
    dependencies
      .map(([d, v]) => ({ [d]: v }))
      .reduce((a, b) => ({ ...a, ...b }))
  );
  await appContainer.ensureDependencies();
})();
