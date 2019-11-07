const TemplatePackage = require("../../TemplatePackage");
const ChartRendererSnippet = require("../../ChartRendererSnippet");

class BizchartTemplate extends TemplatePackage {
  constructor(chartLibrary) {
    super({
      name: 'bizchart-charts',
      fileToSnippet: {
        '/src/components/ChartRenderer.js': new ChartRendererSnippet(chartLibrary)
      },
      type: 'charts'
    });
  }
}

module.exports = BizchartTemplate;
