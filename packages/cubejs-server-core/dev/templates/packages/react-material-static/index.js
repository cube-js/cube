const TemplatePackage = require("../../TemplatePackage");
const AppSnippet = require("../../AppSnippet");
const IndexSnippet = require("../../IndexSnippet");

class ReactMaterialStaticTemplate extends TemplatePackage {
  constructor() {
    super({
      name: 'react-material-static',
      description: 'React material-ui static',
      fileToSnippet: {
        '/src/App.js': new AppSnippet(),
        '/src/index.js': new IndexSnippet(),
      },
      requires: 'create-react-app',
      receives: ['credentials', 'charts', 'static-chart', 'transport', 'material-tables'],
      version: '0.0.1'
    });
  }
}

module.exports = ReactMaterialStaticTemplate;
