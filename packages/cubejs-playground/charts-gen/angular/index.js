const fs = require('fs-extra');
const { TargetSource, utils } = require('@cubejs-templates/core');
const { pascalCase, paramCase } = require('change-case');

const DependencTree = require('../dev/DependencyTree');
const AppContainer = require('../dev/AppContainer');
const DevPackageFetcher = require('../dev/DevPackageFetcher');
const path = require('path');
const { executeCommand } = require('../dev/utils');
const { generateCodeChunks } = require('./code-chunks-gen');

const repo = {
  owner: 'cube-js',
  name: 'cubejs-playground-templates',
};

const chartingLibraryTemplates = ['angular-ng2-charts', 'angular-test-charts'];

const packages = [
  // 'dev-cnga',
  'create-ng-app',
  'angular-charts',
  'ng-credentials',
];

const rootPath = path.resolve(`${__dirname}/../..`);
const distPath = `${rootPath}/charts-dist/angular`;
const angularChartsPath = `${distPath}/angular-charts`;

(async () => {
  const fetcher = new DevPackageFetcher(repo);
  const manifest = await fetcher.manifestJSON();
  const { packagesPath } = await fetcher.downloadPackages();

  const dt = new DependencTree(manifest, packages);

  const appContainer = new AppContainer(
    dt.getRootNode(),
    {
      appPath: angularChartsPath,
      packagesPath,
    },
    {
      credentials: {
        apiUrl: 'http://localhost:4000/cubejs-api/v1',
        cubejsToken: 'secret',
      },
    }
  );

  await appContainer.applyTemplates();

  let dependencies = [];
  const chartingLibraryDependencies = {};
  const chartingLibraryFiles = {};

  try {
    await Promise.all(
      chartingLibraryTemplates.map(async (key) => {
        await executeCommand('cp', [
          '-R',
          `${packagesPath}/${key}/scaffolding/app/query-renderer`,
          `${angularChartsPath}/src/app/${key}`,
        ]);

        const fileContents = await utils.fileContentsRecursive(
          `${angularChartsPath}/src/app/${key}`
        );
        chartingLibraryFiles[key] = fileContents
          .map(({ fileName, content }) => ({
            [`src/app/query-renderer${fileName}`]: content,
          }))
          .reduce((a, b) => ({ ...a, ...b }), {});

        const currentDependencies = fileContents
          .map(({ content, fileName }) => {
            let code = content;
            if (fileName.includes('query-renderer.component.ts')) {
              code = content.replace(
                'class QueryRendererComponent',
                `class ${pascalCase(key)}`
              );
              code = code.replace(
                `selector: 'query-renderer'`,
                `selector: '${paramCase(key)}'`
              );

              fs.writeFileSync(
                `${angularChartsPath}/src/app/${key}/query-renderer.component.ts`,
                code
              );
            }

            const ts = new TargetSource(fileName, code);
            return ts.getImportDependencies();
          })
          .reduce((a, b) => [...a, ...b], []);
        dependencies = dependencies.concat(currentDependencies);

        chartingLibraryDependencies[key] = currentDependencies;
      })
    );
  } catch (error) {
    console.log(error);
  }

  const codeChunks = generateCodeChunks({
    chartingLibraryDependencies,
    chartingLibraryFiles,
  });

  fs.writeFileSync(`${angularChartsPath}/src/code-chunks.ts`, codeChunks);

  appContainer.sourceContainer.addImportDependencies(
    dependencies
      .map((d) => ({ [d]: 'latest' }))
      .reduce((a, b) => ({ ...a, ...b }), [])
  );
  await appContainer.ensureDependencies();
})();
