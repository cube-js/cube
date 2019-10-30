import TemplatePackage from "./TemplatePackage";
import ChartRendererSnippet from "./ChartRendererSnippet";

class ChartRendererTemplate extends TemplatePackage {
  constructor(chartLibrary) {
    super({
      name: 'chart-renderer',
      fileToSnippet: {
        '/src/components/ChartRenderer.js': new ChartRendererSnippet(chartLibrary)
      }
    });
  }
}

export default ChartRendererTemplate;
