const TemplatePackage = require("../../TemplatePackage");
const ChartRendererSnippet = require("../../ChartRendererSnippet");

class D3Template extends TemplatePackage {
  constructor(chartLibrary) {
    super({
      name: 'd3-charts',
      fileToSnippet: {
        '/src/components/ChartRenderer.js': new ChartRendererSnippet(chartLibrary)
      },
      type: 'charts',
      version: '0.0.1'
    });
  }
}

module.exports = D3Template;
