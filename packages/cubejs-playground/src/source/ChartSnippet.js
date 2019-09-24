import traverse from "@babel/traverse";
import * as t from "@babel/types";
import SourceSnippet from './SourceSnippet';

class ChartSnippet extends SourceSnippet {
  findDefinitions() {
    return super.findDefinitions().filter(path => path.get('declarations')[0].get('id').get('name').node !== 'ChartRenderer');
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    const chartRendererPath = super.findDefinitions()
      .find(path => path.get('declarations')[0].get('id').get('name').node === 'ChartRenderer');
    const chartRendererElement = chartRendererPath.get('declarations')[0].get('init').get('body');

    let dashboardElement = null;

    traverse(targetSource.ast, {
      JSXOpeningElement: (path) => {
        if (path.get('name').get('name').node === 'Dashboard') {
          dashboardElement = path;
        }
      }
    });

    if (!dashboardElement.parentPath.get('children').length) {
      dashboardElement.parentPath.replaceWith(t.JSXElement(
        t.JSXOpeningElement(t.JSXIdentifier('Dashboard'), []),
        t.JSXClosingElement(t.JSXIdentifier('Dashboard')),
        []
      ));
      dashboardElement = dashboardElement.parentPath.get('openingElement');
    }

    if (!dashboardElement) {
      throw new Error(`Dashboard not found in ${targetSource.fileName}`);
    }

    dashboardElement.parentPath.pushContainer(
      'children',
      t.JSXElement(
        t.JSXOpeningElement(t.JSXIdentifier('DashboardItem'), []),
        t.JSXClosingElement(t.JSXIdentifier('DashboardItem')),
        [chartRendererElement.node]
      )
    );
  }

  insertAnchor(targetSource) {
    let appClass = null;
    traverse(targetSource.ast, {
      VariableDeclaration: (path) => {
        if (path.get('declarations')[0].get('id').node.name === 'DashboardPage') {
          appClass = path;
        }
      }
    });
    if (!appClass) {
      throw new Error(`DashboardPage class not found. Can't parse dashboard app.  Please delete dashboard-app directory and try to create it again.`);
    }
    return appClass;
  }
}

export default ChartSnippet;
