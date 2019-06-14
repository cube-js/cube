import React from 'react';
import SourceRender from 'react-source-render';
import presetEnv from '@babel/preset-env';
import presetReact from '@babel/preset-react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';
import { Alert } from 'antd';

import ChartContainer from './ChartContainer';
import * as bizChartLibrary from './libraries/bizChart';
import * as chartjsLibrary from './libraries/chartjs';

export const libraryToTemplate = {
  bizcharts: bizChartLibrary,
  chartjs: chartjsLibrary
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
${libraryToTemplate[chartLibrary].sourceCodeTemplate({ ...props, renderFnName })}

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
  query={${(typeof query === 'object' ? JSON.stringify(query, null, 2) : query).split('\n').map((l, i) => i > 0 ? `  ${l}` : l).join('\n')}}
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

export const ChartRenderer = (props) => {
  let {
    query,
    sourceCodeFn,
    title,
    resultSet,
    error,
    sqlQuery,
    chartLibrary,
    dashboardSource,
    cubejsApi
  } = props;
  sourceCodeFn = sourceCodeFn || sourceCodeTemplate;
  const source = sourceCodeFn(props);
  const dependencies = {
    '@cubejs-client/core': cubejs,
    '@cubejs-client/react': cubejsReact,
    antd,
    react: React,
    ...libraryToTemplate[chartLibrary].imports
  };
  return (
    <SourceRender
      babelConfig={babelConfig}
      onError={error => console.log(error)}
      onSuccess={(error, { markup }) => console.log('HTML', markup)}
      resolver={importName => dependencies[importName]}
      source={source}
    >
      <SourceRender.Consumer>
        {({ element, error: jsCompilingError }) => (
          <ChartContainer
            title={title}
            query={query}
            resultSet={resultSet}
            error={error}
            sqlQuery={sqlQuery}
            codeExample={source}
            codeSandboxSource={withDomRender(source)}
            dependencies={dependencies}
            dashboardSource={dashboardSource}
            cubejsApi={cubejsApi}
            render={() => jsCompilingError ? (<Alert
              message="Error occurred while compiling JS"
              description={<pre>{jsCompilingError.toString()}</pre>}
              type="error"
            />) : element}
          />
        )}
      </SourceRender.Consumer>
    </SourceRender>
  );
};