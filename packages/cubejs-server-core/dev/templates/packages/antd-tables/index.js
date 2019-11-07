const TemplatePackage = require("../../TemplatePackage");
const ChartRendererSnippet = require("../../ChartRendererSnippet");

class AntdTablesTemplate extends TemplatePackage {
  constructor(chartLibrary) {
    super({
      name: 'antd-tables',
      fileToSnippet: {
        '/src/components/ChartRenderer.js': new ChartRendererSnippet(chartLibrary)
      },
      type: 'charts'
    });
  }
}

module.exports = AntdTablesTemplate;
