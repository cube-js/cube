const t = require('@babel/types');
const generator = require('@babel/generator').default;
const traverse = require('@babel/traverse').default;
const fs = require('fs-extra');
const {
  SourceSnippet,
  VueMainSnippet,
  TargetSource,
  utils,
} = require('@cubejs-templates/core');
const { pascalCase } = require('change-case');
const path = require('path');

const DependencyTree = require('../dev/DependencyTree');
const AppContainer = require('../dev/AppContainer');
const DevPackageFetcher = require('../dev/DevPackageFetcher');
const { executeCommand } = require('../dev/utils');
const { REPOSITORY } = require('../env');

const chartingLibraryTemplates = ['vue-chartkick-charts', 'vue-chartjs-charts'];
const packages = ['dev-cva', 'vue-charts'];

const rootPath = path.resolve(`${__dirname}/../..`);
const distPath = `${rootPath}/charts-dist/vue`;
const vueChartsPath = `${distPath}/vue-charts`;

function astToCode(ast) {
  return generator(ast, {
    decoratorsBeforeExport: true,
  }).code;
}

(async () => {
  await executeCommand('rm -rf ../../charts-dist/vue', [], {
    shell: true,
    cwd: path.resolve(__dirname),
  });

  const fetcher = new DevPackageFetcher(REPOSITORY);
  const manifest = await fetcher.manifestJSON();
  const { packagesPath } = await fetcher.downloadPackages();

  let dependencies = [['chart.js', '2.9.4']];

  await Promise.all(
    chartingLibraryTemplates.map(async (key) => {
      const dashboardAppPath = `${distPath}/${key}`;
      const dt = new DependencyTree(manifest, ['vue-charting-library', key]);

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
    appPath: vueChartsPath,
    packagesPath,
  });

  await appContainer.applyTemplates();

  // let code = '';
  // const imports = [];
  // const libNames = [];
  //
  const chartRenderers = [];

  await Promise.all(
    chartingLibraryTemplates.map(async (key) => {
      const name = key.split('-')[1];
      if (!name) {
        throw new Error(`Unable to parse name out of '${key}'`);
      }
      const className = pascalCase(`vue-${name}-renderer`);
      chartRenderers.push([name, className, key]);

      // const fileContents = await utils.fileContentsRecursive(
      //   `${distPath}/${key}`
      // );
      //
      // const chartRendererContent = fileContents.find(
      //   ({ fileName }) => fileName === '/src/components/ChartRenderer.js'
      // );
      //
      // const codeChunksContent = generateCodeChunks(
      //   chartRendererContent.content
      // );

      await executeCommand('mv', [
        `${distPath}/${key}/src`,
        `${vueChartsPath}/src/${name}`,
      ]);
      // fs.writeFileSync(
      //   `${vueChartsPath}/src/${key}/src/code-chunks.js`,
      //   codeChunksContent
      // );
      //
      // libNames.push(`${key.replace(/-([^-]+)/, '')}: ${pascalCase(key)}`);
      // imports.push(
      //   `import ${pascalCase(
      //     key
      //   )} from './${key}/src/components/ChartRenderer';`
      // );
    })
  );

  const chartRendererTargetSource = appContainer.sourceContainer.getTargetSource(
    '/src/ChartRenderer.vue'
  );
  const mainTargetSource = appContainer.sourceContainer.getTargetSource(
    '/src/main.js'
  );

  traverse(chartRendererTargetSource.ast, {
    Property(path) {
      if (
        t.isIdentifier(path.node.key) &&
        path.node.key.name === 'components' &&
        t.isObjectExpression(path.node.value)
      ) {
        chartRenderers.forEach(([key, value]) => {
          path.node.value.properties.push(
            t.objectProperty(t.identifier(key), t.stringLiteral(value))
          );
        });
      }
    },
  });

  const importsContent = [];
  chartRenderers.forEach(([key, value]) => {
    importsContent.push(
      `import ${value} from './${key}/components/ChartRenderer';`
    );

    const mainCode = fs.readFileSync(
      path.join(vueChartsPath, 'src', key, 'main.js'),
      'utf-8'
    );
    const mainSnippet = new VueMainSnippet(mainCode);
    mainSnippet.mergeTo(mainTargetSource);
  });

  const importSnippet = new SourceSnippet(importsContent.join('\n'));
  importSnippet.mergeTo(chartRendererTargetSource);

  fs.writeFileSync(
    `${vueChartsPath}/src/ChartRenderer.vue`,
    chartRendererTargetSource.formattedCode()
  );
  fs.writeFileSync(
    `${vueChartsPath}/src/main.js`,
    mainTargetSource.formattedCode()
  );
  await executeCommand(
    'rm ./src/App.vue && mv ./src/ChartContainer.vue src/App.vue',
    [],
    {
      shell: true,
      cwd: vueChartsPath,
    }
  );

  console.log('>>', appContainer.sourceContainer.importDependencies);

  // const libsSnippet = new SourceSnippet(code);
  // const appTarget = new TargetSource(
  //   'app.js',
  //   fs.readFileSync(path.join(vueChartsPath, 'src/App.js'), 'utf-8')
  // );
  //
  // libsSnippet.mergeTo(appTarget);
  // fs.writeFileSync(path.join(vueChartsPath, 'src/App.js'), appTarget.code());
  //
  // appContainer.sourceContainer.addImportDependencies(
  //   dependencies
  //     .map(([d, v]) => ({ [d]: v }))
  //     .reduce((a, b) => ({ ...a, ...b }))
  // );
  await appContainer.ensureDependencies();
})();
