const TemplatePackage = require("./TemplatePackage");
const ChartSnippet = require("./ChartSnippet");

class StaticChartTemplate extends TemplatePackage {
  constructor(chartSource) {
    super({
      name: 'chart',
      fileToSnippet: {
        '/src/pages/DashboardPage.js': new ChartSnippet(chartSource)
      }
    });
  }
}

module.exports = StaticChartTemplate;
