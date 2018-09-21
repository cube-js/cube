import React from 'react';
import ReactDOM from 'react-dom';
import cubejs from 'cubejs-client';
import JSONPretty from 'react-json-pretty';
import { QueryRenderer } from '@cubejs-client/react';
import { Pie } from 'react-chartjs-2';

const HACKER_NEWS_DATASET_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw'
const toChartJsConfig = (resultSet, userConfig = {}) => {
  return {
    labels: resultSet.categories().map(c => c.category),
    datasets: resultSet.series().map(s => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        backgroundColor: ['#FF6492', '#141446', '#7A77FF'],
        hoverBackgroundColor: ['#FF6492', '#141446', '#7A77FF'],
      }
    )),
    ...userConfig
  }
}

const App = () => {
  return (
    <QueryRenderer
      query={{ measures: ['Stories.count'], dimensions: ['Stories.category'] }}
      cubejsApi={cubejs(HACKER_NEWS_DATASET_API_KEY)}
      render={ ({ resultSet, error }) => {
        if (resultSet) {
          return (
            [
              <Pie data={toChartJsConfig(resultSet)} />,
              <JSONPretty id="json-pretty" json={resultSet}></JSONPretty>
            ]
          );
        }
        return <div>Loading</div>;
      }}
    />
  )
}

ReactDOM.render(<App />, document.getElementById('root'));
