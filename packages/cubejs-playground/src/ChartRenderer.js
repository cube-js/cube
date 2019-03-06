import React from 'react';
import ChartContainer from './ChartContainer';
import SourceRender from 'react-source-render';
import presetEnv from '@babel/preset-env';
import presetReact from '@babel/preset-react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';
import { Alert } from 'antd';

import * as bizChartLibrary from './libraries/bizChart';
import * as chartjsLibrary from './libraries/chartjs';

const libraryToTemplate = {
  bizcharts: bizChartLibrary,
  chartjs: chartjsLibrary
};

const babelConfig = {
  presets: [
    presetEnv,
    presetReact
  ]
};

const sourceCodeTemplate = (props) => {
  const { chartLibrary, query, apiUrl, cubejsToken } = props;
  return `import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
${libraryToTemplate[chartLibrary].sourceCodeTemplate(props)}

const query =
${typeof query === 'object' ? JSON.stringify(query, null, 2) : query};

const API_URL = "${apiUrl}"; // change to your actual endpoint

const cubejsApi = cubejs(
  "${cubejsToken}",
  { apiUrl: API_URL + "/cubejs-api/v1" }
);

const ChartRenderer = () => <QueryRenderer
  query={query}
  cubejsApi={cubejsApi}
  render={({ resultSet, error }) => (
    (resultSet && renderChart(resultSet)) ||
    (error && error.toString()) || 
    (<Spin />)
  )}
/>;

export default ChartRenderer;
`};

const withDomRender = (source) => `import ReactDOM from 'react-dom';
${source}

const rootElement = document.getElementById("root");
ReactDOM.render(<ChartRenderer />, rootElement);
`;

const ChartRenderer = (props) => {
  let {
    query,
    sourceCodeFn,
    title,
    resultSet,
    error,
    sqlQuery,
    chartLibrary
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
  return (<SourceRender
    babelConfig={babelConfig}
    onError={error => console.log(error)}
    onSuccess={(error, { markup }) => console.log('HTML', markup)}
    resolver={importName => dependencies[importName]}
    source={source}
  >
    <SourceRender.Consumer>{({ element, error: jsCompilingError }) =>
      <ChartContainer
        title={title}
        query={query}
        resultSet={resultSet}
        error={error}
        sqlQuery={sqlQuery}
        codeExample={source}
        codeSandboxSource={withDomRender(source)}
        dependencies={dependencies}
        render={() => jsCompilingError ? (<Alert
          message="Error occurred while compiling JS"
          description={<pre>{jsCompilingError.toString()}</pre>}
          type="error"
        />) : element}
      />
    }</SourceRender.Consumer>
  </SourceRender>);
};

export default ChartRenderer;
