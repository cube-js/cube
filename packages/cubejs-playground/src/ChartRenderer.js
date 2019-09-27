import React, { useState } from 'react';
import PropTypes from 'prop-types';
import SourceRender from 'react-source-render';
import presetEnv from '@babel/preset-env';
import presetReact from '@babel/preset-react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
// eslint-disable-next-line import/no-duplicates
import * as antd from 'antd';
// eslint-disable-next-line import/no-duplicates
import { Alert } from 'antd';

import ChartContainer from './ChartContainer';
import * as bizChartLibrary from './libraries/bizChart';
import * as chartjsLibrary from './libraries/chartjs';
import * as tablesLibrary from './libraries/tables';

export const libraryToTemplate = {
  bizcharts: { library: bizChartLibrary, title: 'Bizcharts' },
  chartjs: { library: chartjsLibrary, title: 'Chart.js' }
};

export const babelConfig = {
  presets: [
    presetEnv,
    presetReact
  ]
};

const sourceCodeTemplate = (props) => {
  const {
    chartLibrary, query, apiUrl, cubejsToken, chartType
  } = props;
  const renderFnName = `${chartType}Render`;
  return `import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
${chartLibrary.sourceCodeTemplate({ ...props, renderFnName })}

const API_URL = "${apiUrl}"; // change to your actual endpoint

const cubejsApi = cubejs(
  "${cubejsToken}",
  { apiUrl: API_URL + "/cubejs-api/v1" }
);

const renderChart = (Component) => ({ resultSet, error }) => (
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || 
  (<Spin />)
)

const ChartRenderer = () => <QueryRenderer
  query={${(typeof query === 'object' ? JSON.stringify(query, null, 2) : query).split('\n').map((l, i) => (i > 0 ? `  ${l}` : l)).join('\n')}}
  cubejsApi={cubejsApi}
  render={renderChart(${renderFnName})}
/>;

export default ChartRenderer;
`;
};

const withDomRender = (source) => `import ReactDOM from 'react-dom';
${source}

const rootElement = document.getElementById("root");
ReactDOM.render(<ChartRenderer />, rootElement);
`;

export const selectChartLibrary = (chartType, chartLibrary) => {
  return ['table', 'number'].indexOf(chartType) !== -1
    ? tablesLibrary : libraryToTemplate[chartLibrary].library;
};

export const ChartRenderer = (props) => {
  const {
    query,
    resultSet,
    error,
    sqlQuery,
    dashboardSource,
    cubejsApi,
    chartType
  } = props;

  let { sourceCodeFn } = props;

  const [chartLibrary, setChartLibrary] = useState('bizcharts');

  sourceCodeFn = sourceCodeFn || sourceCodeTemplate;
  const selectedChartLibrary = selectChartLibrary(chartType, chartLibrary);
  const source = sourceCodeFn({
    ...props,
    chartLibrary: selectedChartLibrary
  });
  const dependencies = {
    '@cubejs-client/core': cubejs,
    '@cubejs-client/react': cubejsReact,
    antd,
    react: React,
    ...selectedChartLibrary.imports
  };
  return (
    <SourceRender
      babelConfig={babelConfig}
      onError={e => console.log(e)}
      resolver={importName => dependencies[importName]}
      source={source}
    >
      <SourceRender.Consumer>
        {({ element, error: jsCompilingError }) => (
          <ChartContainer
            query={query}
            resultSet={resultSet}
            error={error}
            sqlQuery={sqlQuery}
            codeExample={source}
            codeSandboxSource={withDomRender(source)}
            dependencies={dependencies}
            dashboardSource={dashboardSource}
            chartLibrary={chartLibrary}
            setChartLibrary={setChartLibrary}
            chartLibraries={Object.keys(libraryToTemplate).map(k => ({ value: k, title: libraryToTemplate[k].title }))}
            cubejsApi={cubejsApi}
            render={() => (jsCompilingError ? (
              <Alert
                message="Error occurred while compiling JS"
                description={<pre>{jsCompilingError.toString()}</pre>}
                type="error"
              />
            ) : element)}
          />
        )}
      </SourceRender.Consumer>
    </SourceRender>
  );
};

ChartRenderer.propTypes = {
  query: PropTypes.object.isRequired,
  resultSet: PropTypes.object,
  error: PropTypes.object,
  sqlQuery: PropTypes.object,
  dashboardSource: PropTypes.object,
  cubejsApi: PropTypes.object,
  chartType: PropTypes.string,
  sourceCodeFn: PropTypes.func
};

ChartRenderer.defaultProps = {
  resultSet: null,
  error: null,
  sqlQuery: null,
  dashboardSource: null,
  cubejsApi: null,
  chartType: null,
  sourceCodeFn: null
};
