/**
 * Runs `extractArchive` from the compiled CommonJS output in a real Node process.
 *
 * This deliberately lives outside the Jest module runtime: Jest intercepts dynamic
 * `import()` inside CommonJS modules and resolves it through its own CommonJS loader,
 * which cannot load the ESM-only `@xhmikosr/decompress`. Running it as a plain `node`
 * child process is what makes the CommonJS -> ESM hop actually observable.
 */
const { extractArchive } = require('../../dist/src/index.js');

const [archivePath, outDir] = process.argv.slice(2);

extractArchive(archivePath, outDir).catch((err) => {
  console.error(err);
  process.exit(1);
});
