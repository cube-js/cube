const fs = require('fs');
const { join } = require('path');
// symlink to self for running local examples/tests
const pluginPath = join(__dirname, '..', 'node_modules/typedoc-plugin-markdown');
if (!fs.existsSync(pluginPath)) {
  fs.symlinkSync(join(__dirname, '..'), pluginPath);
}
