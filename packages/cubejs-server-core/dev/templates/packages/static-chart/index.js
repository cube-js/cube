const TemplatePackage = require("../../TemplatePackage");
const ChartSnippet = require("../../ChartSnippet");

class StaticChartTemplate extends TemplatePackage {
  constructor({ chartCode }) {
    super({
      name: 'static-chart',
      fileToSnippet: {
        '/src/pages/DashboardPage.js': new ChartSnippet(chartCode)
      },
      version: '0.0.1'
    });
  }
}

module.exports = StaticChartTemplate;
