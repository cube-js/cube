import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';
import { selectChartLibrary } from "../ChartRenderer";
import ChartTypeSnippet from "./ChartTypeSnippet";

class ChartRendererSnippet extends SourceSnippet {
  constructor(chartLibrary) {
    super(ScaffoldingSources['react/components/ChartRenderer.js']);
    this.chartLibrary = chartLibrary;
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    const chartTypes = ['line', 'bar', 'area', 'pie', 'table', 'number'];
    chartTypes.forEach(chartType => {
      const chartLibrary = selectChartLibrary(chartType, this.chartLibrary);
      const chartSnippet = new ChartTypeSnippet(
        chartLibrary.sourceCodeTemplate({ chartType, renderFnName: 'render' }),
        chartType
      );
      chartSnippet.mergeTo(targetSource);
    });
  }
}

export default ChartRendererSnippet;
