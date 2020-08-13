import React, { useEffect, useState } from 'react';
import PropTypes from 'prop-types';
import SourceRender from 'react-source-render';
import presetEnv from '@babel/preset-env';
import presetReact from '@babel/preset-react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
// eslint-disable-next-line import/no-duplicates
// eslint-disable-next-line import/no-duplicates
import * as antd from 'antd';
import { Alert } from 'antd';

import ChartContainer from './ChartContainer';
import * as bizChartLibrary from './libraries/bizChart';
import * as rechartsLibrary from './libraries/recharts';
import * as chartjsLibrary from './libraries/chartjs';
import * as d3ChartLibrary from './libraries/d3';
import * as tablesLibrary from './libraries/tables';

export const libraryToTemplate = {
  chartjs: { library: chartjsLibrary, title: 'Chart.js' },
  recharts: { library: rechartsLibrary, title: 'Recharts' },
  bizcharts: { library: bizChartLibrary, title: 'Bizcharts' },
  d3: { library: d3ChartLibrary, title: 'D3' },
};

export const babelConfig = {
  presets: [presetEnv, presetReact],
};

const prettify = (object) => {
  let str = object;
  if (typeof object === 'object') {
    str = JSON.stringify(object, null, 2);
  }

  return str
    .split('\n')
    .map((l, i) => (i > 0 ? `  ${l}` : l))
    .join('\n');
};

const sourceCodeTemplate = (props) => {
  const { chartLibrary, query, apiUrl, cubejsToken, chartType } = props;
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

const renderChart = (Component, pivotConfig) => ({ resultSet, error }) => (
  (resultSet && <Component resultSet={resultSet} pivotConfig={pivotConfig} />) ||
  (error && error.toString()) || 
  (<Spin />)
)

const ChartRenderer = () => <QueryRenderer
  query={${prettify(query)}}
  cubejsApi={cubejsApi}
  render={renderChart(${renderFnName}, ${prettify(props.pivotConfig)})}
/>;

export default ChartRenderer;
`;
};

const forCodeSandBox = (source) => `import ReactDOM from 'react-dom';
import "antd/dist/antd.css";
${source}

const rootElement = document.getElementById("root");
ReactDOM.render(<ChartRenderer />, rootElement);
`;

export const selectChartLibrary = (chartType, chartLibrary) =>
  ['table', 'number'].indexOf(chartType) !== -1
    ? tablesLibrary
    : libraryToTemplate[chartLibrary].library;

export const chartLibraries = Object.keys(libraryToTemplate).map((k) => ({
  value: k,
  title: libraryToTemplate[k].title,
}));

export const ChartRenderer = (props) => {
  const [jsCompilingError, setError] = useState(null);
  const [chartLibrary, setChartLibrary] = useState('bizcharts');

  const {
    query,
    resultSet,
    error,
    sqlQuery,
    dashboardSource,
    cubejsApi,
    chartType,
    sourceCodeFn: sourceCodeFnProp,
    pivotConfig,
  } = props;

  const sourceCodeFn = sourceCodeFnProp || sourceCodeTemplate;

  const selectedChartLibrary = selectChartLibrary(chartType, chartLibrary);
  const source = sourceCodeFn({
    ...props,
    chartLibrary: selectedChartLibrary,
    pivotConfig,
  });
  const dependencies = {
    '@cubejs-client/core': cubejs,
    '@cubejs-client/react': cubejsReact,
    antd,
    react: React,
    ...selectedChartLibrary.imports,
  };

  useEffect(() => {
    if (jsCompilingError) {
      setError(null);
    }
  }, [source, chartType, jsCompilingError]);

  return (
    <ChartContainer
      query={query}
      resultSet={resultSet}
      error={error}
      sqlQuery={sqlQuery}
      codeExample={source}
      codeSandboxSource={forCodeSandBox(source)}
      dependencies={dependencies}
      dashboardSource={dashboardSource}
      chartLibrary={chartLibrary}
      setChartLibrary={setChartLibrary}
      chartLibraries={chartLibraries}
      cubejsApi={cubejsApi}
      render={() => {
        if (jsCompilingError) {
          return (
            <Alert
              message="Error occurred while compiling JS"
              description={<pre>{jsCompilingError.toString()}</pre>}
              type="error"
            />
          );
        }

        return (
          <SourceRender
            onRender={(renderError) => {
              if (renderError) {
                setError(renderError);
              }
            }}
            babelConfig={babelConfig}
            resolver={(importName) => dependencies[importName]}
            source={source}
          />
        );
      }}
    />
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
  sourceCodeFn: PropTypes.func,
};

ChartRenderer.defaultProps = {
  resultSet: null,
  error: null,
  sqlQuery: null,
  dashboardSource: null,
  cubejsApi: null,
  chartType: null,
  sourceCodeFn: null,
};
