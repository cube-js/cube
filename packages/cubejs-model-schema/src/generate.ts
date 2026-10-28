// Writes the package's published files: `manifests/<version>.json`, the `index.json` that names
// it, and `index.js`, the entry point that returns the latest manifest.
import fs from 'fs';
import path from 'path';
import { describeModelSchemas } from '@cubejs-backend/schema-compiler';

import { modelSchemaFromJoi } from './converter';

const MANIFEST_FORMAT_VERSION = 1;

const root = path.resolve(__dirname, '../..');
const { version } = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
const manifest = `manifests/${version}.json`;

const { schema, stats } = modelSchemaFromJoi(describeModelSchemas());

fs.rmSync(path.join(root, 'manifests'), { recursive: true, force: true });
fs.mkdirSync(path.join(root, 'manifests'));
fs.writeFileSync(path.join(root, manifest), `${JSON.stringify(schema, null, 2)}\n`);
fs.writeFileSync(path.join(root, 'index.json'), `${JSON.stringify({
  manifestFormatVersion: MANIFEST_FORMAT_VERSION,
  latest: version,
  manifests: { [version]: manifest },
}, null, 2)}\n`);
fs.writeFileSync(path.join(root, 'index.js'), `module.exports = require('./${manifest}');\n`);

console.log(`Wrote ${manifest}`, stats);
