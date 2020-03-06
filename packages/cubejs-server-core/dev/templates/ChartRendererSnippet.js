const traverse = require("@babel/traverse").default;
const SourceSnippet = require("./SourceSnippet");

class ChartRendererSnippet extends SourceSnippet {
  constructor(chartLibrary) {
    super();
    this.chartLibrary = chartLibrary;
  }

  handleExistingMerge(existingDefinition, newDefinition) {
    if (existingDefinition.get('id').node.name === 'TypeToChartComponent') {
      const existingPropertyNames = existingDefinition.get('init').get('properties').map(p => p.node.key.name);
      existingDefinition.get('init').pushContainer(
        'properties',
        newDefinition.node.init.properties.filter(p => existingPropertyNames.indexOf(p.key.name) === -1)
      );
    } else {
      super.handleExistingMerge(existingDefinition, newDefinition);
    }
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

module.exports = ChartRendererSnippet;
