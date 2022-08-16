// Parametrize schemas. Copy outside /schema/ dir.
// See https://cube.dev/docs/schema/reference/execution-environment#node-js-globals-process-env-console-log-and-others

for (const k of Object.keys(process.env)) {
  if (k.startsWith('CUBEJS_')) {
    exports[k] = process.env[k];
  }
}
