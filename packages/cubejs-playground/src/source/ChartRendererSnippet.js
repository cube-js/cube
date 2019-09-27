import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';
import { selectChartLibrary } from "../ChartRenderer";
import ChartTypeSnippet from "./ChartTypeSnippet";

class ChartRendererSnippet extends SourceSnippet {
  constructor() {
    super(ScaffoldingSources['react/ChartRenderer.js']);
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    const chartTypes = ['line', 'bar', 'area', 'pie', 'table', 'number'];
    const chartLibraryId = 'bizcharts'; // TODO
    chartTypes.forEach(chartType => {
      const chartLibrary = selectChartLibrary(chartType, chartLibraryId);
      const chartSnippet = new ChartTypeSnippet(
        chartLibrary.sourceCodeTemplate({ chartType, renderFnName: 'render' }),
        chartType
      );
      chartSnippet.mergeTo(targetSource);
    });
  }
}

export default ChartRendererSnippet;
