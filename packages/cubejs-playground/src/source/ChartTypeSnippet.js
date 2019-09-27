import traverse from "@babel/traverse";
import * as t from "@babel/types";
import SourceSnippet from './SourceSnippet';

class ChartTypeSnippet extends SourceSnippet {
  constructor(source, chartType) {
    super(source);
    this.chartType = chartType;
  }

  findDefinitions() {
    return super.findDefinitions().filter(path => path.get('declarations')[0].get('id').get('name').node !== 'render');
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    const chartRendererPath = super.findDefinitions()
      .find(path => path.get('declarations')[0].get('id').get('name').node === 'render');
    const typeToChartComponent = targetSource.definitions.find(d => d.get('id').get('name').node === 'TypeToChartComponent');
    typeToChartComponent.get('init').pushContainer(
      'properties',
      t.objectProperty(t.identifier(this.chartType), chartRendererPath.get('declarations')[0].get('init').node)
    );
  }

  insertAnchor(targetSource) {
    let anchor = null;
    traverse(targetSource.ast, {
      VariableDeclaration: (path) => {
        if (path.get('declarations')[0].get('id').node.name === 'TypeToChartComponent') {
          anchor = path;
        }
      }
    });
    if (!anchor) {
      throw new Error(`renderChart class not found. Can't parse dashboard app.  Please delete dashboard-app directory and try to create it again.`);
    }
    return anchor;
  }
}

export default ChartTypeSnippet;
