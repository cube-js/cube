const TemplatePackage = require("../../TemplatePackage");
const ChartSnippet = require("../../ChartSnippet");

class StaticChartTemplate extends TemplatePackage {
  constructor({ chartCode }) {
    super({
      name: 'static-chart',
      fileToSnippet: {
        '/src/pages/DashboardPage.js': new ChartSnippet(chartCode)
      },
      multiPackage: true
    });
  }
}

module.exports = StaticChartTemplate;
