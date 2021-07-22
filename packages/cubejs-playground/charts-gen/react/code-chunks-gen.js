const { TargetSource, SourceSnippet } = require('@cubejs-templates/core');
const t = require('@babel/types');
const traverse = require('@babel/traverse').default;
const generator = require('@babel/generator').default;

function generateCodeChunks(chartRendererCode) {
  const ts = new TargetSource('ChartRenderer.js', chartRendererCode);

  const imports = ts.imports.map((path) => {
    if (!path.get('source').node.value.startsWith('.')) {
      return generator(path.node, {}).code;
    }

    return false;
  }).filter(Boolean);

  const chartComponents = getChartComponents(ts).map(({ key, code }) => {
    return `
      if (chartType === '${key}') {
        return \`${SourceSnippet.formatCode(code)}\`;
      }
    `;
  });

  traverse(ts.ast, {
    ImportDeclaration(path) {
      path.remove();
    },
    ExportDefaultDeclaration(path) {
      path.remove();
    },
    VariableDeclaration(path) {
      if (
        path.get('declarations')[0].get('id').node.name ===
        'TypeToChartComponent'
      ) {
        path.remove();
      }
    },
  });

  return `
    const imports = ${JSON.stringify(imports)};
    
    export function getChartComponent(chartType) {
      ${chartComponents.join('\n')}
    }
    
    export function getCommon() {
      return \`${SourceSnippet.formatCode(ts.code())}\`;
    }
    
    export function getImports() {
      return imports;
    }
  `;
}

function getChartComponents(ts) {
  let anchor = null;
  const codeChunks = [];

  traverse(ts.ast, {
    VariableDeclaration: (path) => {
      if (
        path.get('declarations')[0].get('id').node.name ===
        'TypeToChartComponent'
      ) {
        anchor = path;
      }
    },
  });

  anchor &&
    traverse(
      anchor.node,
      {
        ObjectProperty(path) {
          if (path.parent.type === 'ObjectExpression') {
            let code = '';

            if (
              Array.isArray(path.node.value.body && path.node.value.body.body)
            ) {
              code = generator(t.program(path.node.value.body.body), {}).code;
            } else {
              code = generator(path.node.value.body, {}).code;
            }

            codeChunks.push({
              key: path.node.key.name,
              code,
            });
          }
        },
      },
      anchor.scope,
      anchor.state,
      anchor.parentPath
    );

  return codeChunks;
}

module.exports = {
  generateCodeChunks,
};
