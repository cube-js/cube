const TypeDoc = require('typedoc');
const path = require('path');
const fs = require('fs-extra');

const outputDir = '../docs/docs-new/pages/reference/frontend';

const app = new TypeDoc.Application();

app.options.addReader(new TypeDoc.TSConfigReader());

app.bootstrap({
  excludeExternals: true,
  includeDeclarations: true,
  plugin: ['./dist/index.js'],
  hideSources: true,
  hideIndexes: true,
  name: 'docs',
});

const projects = [
  {
    name: '@cubejs-client-core',
    docsPath: '../packages/cubejs-client-core/index.d.ts',
    outputDir,
  },
  {
    name: '@cubejs-client-react',
    docsPath: '../packages/cubejs-client-react/index.d.ts',
    outputDir,
  },
  {
    name: '@cubejs-client-ws-transport',
    docsPath: '../packages/cubejs-client-ws-transport/index.d.ts',
    outputDir,
  },
];

let failure = false;

projects.forEach(({ name, docsPath, outputDir }) => {
  const tmpDir = path.join(outputDir, 'tmp');
  const project = app.convert(app.expandInputFiles([docsPath]));

  if (project) {
    app.generateDocs(project, tmpDir);

    if (fs.existsSync(tmpDir)) {
      const [tmpFileName] = fs.readdirSync(tmpDir);

      const pathArr = tmpDir.split('/');
      pathArr.splice(-1, 1);
      const out = path.join(...pathArr);
      const currentPath = path.join(out, `${name.replace('@', '')}.mdx`);

      fs.copyFileSync(path.join(tmpDir, tmpFileName), currentPath);
      fs.removeSync(tmpDir);
    }
  } else {
    console.error(`Error while generating '${name}' docs`);
    failure = true;
  }
});

process.exit(failure ? 1 : 0);
