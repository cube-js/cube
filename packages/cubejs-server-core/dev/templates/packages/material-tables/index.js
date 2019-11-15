const TemplatePackage = require("../../TemplatePackage");
const ChartRendererSnippet = require("../../ChartRendererSnippet");

class MaterialTablesTemplate extends TemplatePackage {
  constructor(chartLibrary) {
    super({
      name: 'material-tables',
      fileToSnippet: {
        '/src/components/ChartRenderer.js': new ChartRendererSnippet(chartLibrary)
      },
      version: '0.0.1'
    });
  }
}

module.exports = MaterialTablesTemplate;
