const TemplatePackage = require("../../TemplatePackage");
const ChartRendererSnippet = require("../../ChartRendererSnippet");

class RechartsTemplate extends TemplatePackage {
  constructor(chartLibrary) {
    super({
      name: 'recharts-charts',
      fileToSnippet: {
        '/src/components/ChartRenderer.js': new ChartRendererSnippet(chartLibrary)
      },
      type: 'charts',
      version: '0.0.1'
    });
  }
}

module.exports = RechartsTemplate;
