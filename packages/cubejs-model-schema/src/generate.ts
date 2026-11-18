// Writes the manifest, `manifests/<version>.json`, with `index.json` naming it and `index.js`
// returning it.
import fs from 'fs';
import path from 'path';
import { describeModelSchemas } from '@cubejs-backend/schema-compiler';

import { modelSchemaFromJoi } from './converter';
import { packageFiles } from './package-files';

const root = path.resolve(__dirname, '../..');
const { version } = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));

const { schema, stats } = modelSchemaFromJoi(describeModelSchemas());
const { manifest, manifestText, indexText } = packageFiles(schema, version);

fs.rmSync(path.join(root, 'manifests'), { recursive: true, force: true });
fs.mkdirSync(path.join(root, 'manifests'));
fs.writeFileSync(path.join(root, manifest), manifestText);
fs.writeFileSync(path.join(root, 'index.json'), indexText);
fs.writeFileSync(path.join(root, 'index.js'), `module.exports = require('./${manifest}');\n`);

console.log(`Wrote ${manifest}`, stats);
