import React from 'react';
import { useState, useEffect } from 'react'
import * as classes from './index.module.css'
import * as ReactDOM from 'react-dom/client';
import {
  ApolloClient,
  InMemoryCache,
  ApolloProvider,
  useQuery,
  gql,
  createHttpLink,
  ApolloLink,
  from,
} from '@apollo/client';
import { setContext } from '@apollo/client/link/context';
import {
  tablePivotCube,
  tablePivotApollo,
  availableStepRanges,
  defaultIsFraudSelection,
  defaultStepSelection,
  DisplayFraudAmountSum,
  randomIntFromInterval,
} from './utils/utils';

const httpLink = createHttpLink({
  uri: `${process.env.GRAPHQL_API_URL}`,
});
const authLink = setContext((_, { headers }) => {
  return {
    headers: {
      ...headers,
      'Authorization': `${process.env.AUTHORIZATION}`,
    }
  }
});
let timestampsGlobal = {};
const roundTripLink = new ApolloLink((operation, forward) => {
  operation.setContext({ start: new Date() });
  timestampsGlobal = {};

  return forward(operation).map((data) => {
    timestampsGlobal[operation.operationName] = new Date() - operation.getContext().start;
    return data;
  });
});
const client = new ApolloClient({
  link: from([roundTripLink, authLink.concat(httpLink)]),
  cache: new InMemoryCache()
});

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(
    <ApolloProvider client={client}>
      <App />
    </ApolloProvider>,
  )

function App() {
  const [ timestamps, setTimestamps ] = useState(0);
  useEffect(() => {
    setTimestamps(timestampsGlobal)
  }, [ timestampsGlobal ]);

  const [ fraudChartDataCube, setFraudChartDataCube ] = useState([])
  const [ fraudChartDataApollo, setFraudChartDataApollo ] = useState([])
  const [ stepSelection, setStepSelection ] = useState(defaultStepSelection);
  const selectedStep = availableStepRanges.find(x => x.id === stepSelection);
  const [ isFraudSelection, setIsFraudSelection ] = useState(defaultIsFraudSelection);
  const shuffleAndRun = () => {
    setStepSelection(randomIntFromInterval(1, 14));
    setIsFraudSelection(randomIntFromInterval(0, 1));
  }

  const GET_FRAUD_AMOUNT_SUM_CUBE = gql`
    query CubeQuery { 
      cube(
        where: {fraud: {AND: [
          {step: {gte: ${selectedStep.start} }},
          {step: {lte: ${selectedStep.end} }},
          {isFraud: {equals: "${isFraudSelection}" }}
        ]}},
        orderBy: {fraud: {step: asc}}
      ) {
        fraud {
          amountSum
          step
          type
        }
      }
    }
  `;
  const {
    loading: loadingFraudDataCube,
    error: errorFraudDataCube,
    data: fraudDataCube,
  } = useQuery(GET_FRAUD_AMOUNT_SUM_CUBE);
  useEffect(() => {
    if (loadingFraudDataCube) { return; }
    setFraudChartDataCube(tablePivotCube(fraudDataCube));
  }, [ fraudDataCube ]);

  const GET_FRAUD_AMOUNT_SUM_APOLLO = gql`
    query ApolloQuery {
      fraudsByAmountSumWithStep(
        isFraud: ${isFraudSelection},
        stepStart: ${selectedStep.start},
        stepEnd: ${selectedStep.end}
      ) {
        step
        type
        amountsum
      }
    }
  `;
  const {
    loading: loadingFraudDataApollo,
    error: errorFraudDataApollo,
    data: fraudDataApollo,
  } = useQuery(GET_FRAUD_AMOUNT_SUM_APOLLO);
  useEffect(() => {
    if (loadingFraudDataApollo) { return; }
    setFraudChartDataApollo(tablePivotApollo(fraudDataApollo.fraudsByAmountSumWithStep));
  }, [ fraudDataApollo ]);

  return <>
    <div style={{display: 'flex', justifyContent: 'center'}}>
      <select
        className={classes.select}
        value={stepSelection}
        onChange={e => setStepSelection(parseInt(e.target.value))}
      >
        <option value="" disabled>Select transaction step in time...</option>
        {availableStepRanges.map(stepRange => (
          <option key={stepRange.id} value={stepRange.id}>
            Transactions from {stepRange.start} to {stepRange.end}
          </option>
        ))}
      </select>
      <select
        className={classes.select}
        value={isFraudSelection}
        onChange={e => setIsFraudSelection(parseInt(e.target.value))}
      >
        <option value="" disabled>Select if it's a fraudulent transaction...</option>
        <option key={0} value={0}>
          Non-fraudulent transactions
        </option>
        <option key={1} value={1}>
          Fraudulent transactions
        </option>
      </select>
      <div className={`${classes.buttonwrp}`}>
        <button className={`Button Button--size-s Button--pink`} onClick={shuffleAndRun}>
          Shuffle and Run!
        </button>
      </div>
    </div>

    <table style={{ width: '100%' }}>
      <tbody>
        <tr>
          <td style={{ width: '50%' }}>
            <div style={{ height: '375px', margin: '20px 0' }}>
              <h3 style={{display: 'flex', justifyContent: 'center'}}>Cube {timestamps.CubeQuery ? `(${(timestamps.CubeQuery / 1000).toFixed(2)}s)` : ``}</h3>
              <DisplayFraudAmountSum
                loading={loadingFraudDataCube}
                error={errorFraudDataCube}
                chartData={fraudChartDataCube}
              />
            </div>
          </td>
          <td style={{ width: '50%' }}>
            <div style={{ height: '375px', margin: '20px 0' }}>
              <h3 style={{display: 'flex', justifyContent: 'center'}}>PostgreSQL {timestamps.ApolloQuery ? `(${(timestamps.ApolloQuery / 1000).toFixed(2)}s)` : `(...)`}</h3>
              <DisplayFraudAmountSum
                loading={loadingFraudDataApollo}
                error={errorFraudDataApollo}
                chartData={fraudChartDataApollo}
              />
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </>
}
