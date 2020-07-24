/**
 * author don
 * use by ide
 * @type {module:path}
 */

const path = require('path');
module.exports = {
  resolve: {
    alias: {
      '~': path.resolve(__dirname, "src"),
    }
  }
};
