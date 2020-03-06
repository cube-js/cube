const traverse = require("@babel/traverse").default;
const t = require("@babel/types");
const SourceSnippet = require("./SourceSnippet");

class ChartSnippet extends SourceSnippet {
  mergeTo(targetSource) {
    const dashboardItemsArray = targetSource.definitions.find(
      d => d.get('id').node.type === 'Identifier' &&
        d.get('id').node.name === 'DashboardItems'
    );

    if (!dashboardItemsArray) {
      throw new Error(`DashboardItems array not found. Please use adding chart feature only with static dashboard`);
    }

    traverse(this.ast, {
      JSXOpeningElement: (path) => {
        if (path.get('name').get('name').node === 'QueryRenderer') {
          const query = path.get('attributes').find(p => p.get('name').get('name').node === 'query').get('value').get('expression');
          const rendererCall = path.get('attributes').find(p => p.get('name').get('name').node === 'render').get('value').get('expression');
          const chartType = rendererCall.get('arguments')[0].get('name').node.match(/^([a-zA-Z0-9_)]+)Render$/)[1];
          dashboardItemsArray.get('init').pushContainer(
            'elements',
            t.objectExpression([
              t.objectProperty(t.identifier('id'), t.numericLiteral(dashboardItemsArray.get('init').get('elements').length)),
              t.objectProperty(t.identifier('name'), t.stringLiteral("New Chart")),
              t.objectProperty(t.identifier('vizState'), t.objectExpression([
                t.objectProperty(t.identifier('query'), query.node),
                t.objectProperty(t.identifier('chartType'), t.stringLiteral(chartType))
              ]))
            ])
          );
        }
      }
    });
  }
}

module.exports = ChartSnippet;
