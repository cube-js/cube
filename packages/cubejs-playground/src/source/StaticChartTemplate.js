import TemplatePackage from "./TemplatePackage";
import ChartSnippet from "./ChartSnippet";

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

export default StaticChartTemplate;
