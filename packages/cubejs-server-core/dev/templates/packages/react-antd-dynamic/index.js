const TemplatePackage = require("../../TemplatePackage");
const AppSnippet = require("../../AppSnippet");
const IndexSnippet = require("../../IndexSnippet");

class ReactAntdDynamicTemplate extends TemplatePackage {
  constructor() {
    super({
      name: 'react-antd-dynamic',
      description: 'React antd dynamic',
      fileToSnippet: {
        '/src/App.js': new AppSnippet(),
        '/src/index.js': new IndexSnippet()
      },
      requires: 'create-react-app',
      receives: ['credentials', 'charts', 'transport', 'antd-tables'],
      version: '0.0.1'
    });
  }
}

module.exports = ReactAntdDynamicTemplate;
