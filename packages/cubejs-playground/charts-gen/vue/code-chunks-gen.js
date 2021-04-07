const { VueMainSnippet, TargetSource } = require('@cubejs-templates/core');
const { uniq } = require('ramda');

const chartRenderer = `
<template>
  <query-renderer :cubejs-api="cubejsApi" :query="query">
    <template #default="{ resultSet }">
      ##ChartTemplate##
    </template>
  </query-renderer>
</template>

<script>
import cubejs from "@cubejs-client/core";
import { QueryRenderer } from "@cubejs-client/vue";

const cubejsApi = cubejs('##Token##', {
  apiUrl: '##ApiUrl##',
});

export default {
  name: "ChartRenderer",
  data() {
    return {
      cubejsApi,
      query: '##Query##',
      chartType: '##ChartType##',
      pivotConfig: '##PivotConfig##',
    };
  },
  components: {
    QueryRenderer,
  },
  computed: {    
  },
  methods: {
  },
};
</script>
`;

const mainJs = `
import Vue from "vue";
import ChartRenderer from "./components/ChartRenderer.vue";

Vue.config.productionTip = false;

new Vue({
  render: (h) => h(ChartRenderer)
}).$mount("#app");
`;

const varMap = [
  ['##Token##', 'cubejsToken', 'string'],
  ['##ApiUrl##', 'apiUrl', 'string'],
  ['##Query##', 'query'],
  ['##PivotConfig##', 'pivotConfig'],
  ['##ChartType##', 'chartType', 'string'],
];

const versionedDeps = {
  'vue-chartkick': '^0.6.0',
  'chart.js': '^2.9.4',
};

function generateCodeChunks(sourceContainers) {
  const filesByLibrary = {};
  const dependenciesByLibrary = {};

  sourceContainers.forEach(([libraryName, container]) => {
    if (dependenciesByLibrary[libraryName] === undefined) {
      dependenciesByLibrary[libraryName] = [];
    }

    filesByLibrary[libraryName] = Object.entries(container.fileToTargetSource)
      .map(([filePath, targetSource]) => {
        if (filePath === '/src/main.js') {
          const mainJsTargetSource = new TargetSource('main.js', mainJs);
          const snippet = new VueMainSnippet(targetSource.snippet.source);
          snippet.mergeTo(mainJsTargetSource);

          dependenciesByLibrary[libraryName] = dependenciesByLibrary[
            libraryName
          ].concat(mainJsTargetSource.getImportDependencies());

          return {
            [filePath]: mainJsTargetSource.formattedCode(),
          };
        } else if (filePath === '/src/components/ChartRenderer.vue') {
          let source = chartRenderer.replace(
            '##ChartTemplate##',
            targetSource.snippet.templateSource
          );

          const target = new TargetSource('ChartRenderer.vue', source);
          targetSource.snippet.mergeTo(target);

          source = target.formattedCode();

          varMap.forEach(([name, value, type]) => {
            const replacee = type === 'string' ? `${name}` : `'${name}'`;
            source = source.replace(
              replacee,
              ['${props.', value, '}'].join('')
            );
          });

          dependenciesByLibrary[libraryName] = dependenciesByLibrary[
            libraryName
          ].concat(target.getImportDependencies());

          return {
            [filePath]: source,
          };
        } else {
          dependenciesByLibrary[libraryName] = dependenciesByLibrary[
            libraryName
          ].concat(targetSource.getImportDependencies());

          return {
            [filePath]: targetSource.formattedCode(),
          };
        }
      })
      .reduce((a, b) => ({ ...a, ...b }));
  });

  const deps = Object.entries(dependenciesByLibrary).reduce(
    (memo, [library, dependencies]) => {
      return {
        ...memo,
        [library]: uniq(dependencies).map((d) =>
          versionedDeps[d] ? [d, versionedDeps[d]] : d
        ),
      };
    },
    {}
  );

  const chunks = Object.entries(filesByLibrary).map(([libraryName, files]) => {
    return `
      if (chartingLibrary === '${libraryName}') {
        return { ${Object.entries(files)
          .map(([name, content]) => `'${name.replace(/^\//, '')}': \`${content}\``)
          .join(',')} };
      }
    `;
  });

  return `
    const dependeciesByLibrary = ${JSON.stringify(deps)};
    
    export function getCodesandboxFiles(chartingLibrary, props) {
      ${chunks.join('\n')}
    }
    
    export function getDependencies(chartingLibrary) {
      return dependeciesByLibrary[chartingLibrary];
    }
  `;
}

module.exports = {
  generateCodeChunks,
};
