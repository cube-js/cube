import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';

class ChartRendererSnippet extends SourceSnippet {
  constructor() {
    super(ScaffoldingSources['react/ChartRenderer.js']);
  }
}

export default ChartRendererSnippet;
