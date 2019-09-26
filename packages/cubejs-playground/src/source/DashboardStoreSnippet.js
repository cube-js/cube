import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';
import { selectChartLibrary } from "../ChartRenderer";
import ChartTypeSnippet from "./ChartTypeSnippet";

class DashboardStoreSnippet extends SourceSnippet {
  constructor() {
    super(ScaffoldingSources['react/DashboardStore.js']);
  }
}

export default DashboardStoreSnippet;
