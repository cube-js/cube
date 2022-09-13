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
  range,
  tablePivotCube,
  tablePivotHasura,
  availableStepRanges,
  defaultIsFraudSelection,
  defaultStepSelection,
  DisplayFraudAmountSum,
  randomIntFromInterval,
} from './utils/utils';

const httpLink = createHttpLink({
  uri: `${process.env.HASURA_GRAPHQL_API_URL}`,
});
const authLink = setContext((_, { headers }) => {
  return {
    headers: {
      ...headers,
      'x-hasura-role': `${process.env.X_HASURA_ROLE}`,
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
  const [ fraudChartDataHasura, setFraudChartDataHasura ] = useState([])
  const [ stepSelection, setStepSelection ] = useState(defaultStepSelection);
  const selectedStep = availableStepRanges.find(x => x.id === stepSelection);
  const selectedStepRange = range(selectedStep.start, selectedStep.end);
  const [ isFraudSelection, setIsFraudSelection ] = useState(defaultIsFraudSelection);
  const shuffleAndRun = () => {
    setStepSelection(randomIntFromInterval(1, 14));
    setIsFraudSelection(randomIntFromInterval(0, 1));
  }

  const GET_FRAUD_AMOUNT_SUM_CUBE_REMOTE_SCHEMA = gql`
    query CubeQuery  { 
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
  } = useQuery(GET_FRAUD_AMOUNT_SUM_CUBE_REMOTE_SCHEMA);
  useEffect(() => {
    if (loadingFraudDataCube) { return; }
    setFraudChartDataCube(tablePivotCube(fraudDataCube));
  }, [ fraudDataCube ]);

  const GET_FRAUD_AMOUNT_SUM_HASURA_FRAUDS = gql`
    query HasuraQuery{
      fraud_amount_sum_frauds(
        where: {
          fraud__step: {_in: [${selectedStepRange}]}
        }
        order_by: { fraud__step: asc }
      ) {
        fraud__amount_sum
        fraud__step
        fraud__type
      }
    }
  `;
  const GET_FRAUD_AMOUNT_SUM_HASURA_NON_FRAUDS = gql`
    query HasuraQuery{
      fraud_amount_sum_non_frauds(
        where: {
          fraud__step: {_in: [${selectedStepRange}]}
        }
        order_by: { fraud__step: asc }
      ) {
        fraud__amount_sum
        fraud__step
        fraud__type
      }
    }
  `;
  let GET_FRAUD_AMOUNT_SUM_HASURA;
  if (isFraudSelection) {
    GET_FRAUD_AMOUNT_SUM_HASURA = GET_FRAUD_AMOUNT_SUM_HASURA_FRAUDS;
  } else {
    GET_FRAUD_AMOUNT_SUM_HASURA = GET_FRAUD_AMOUNT_SUM_HASURA_NON_FRAUDS;
  }

  const {
    loading: loadingFraudDataHasura,
    error: errorFraudDataHasura,
    data: fraudDataHasura,
  } = useQuery(GET_FRAUD_AMOUNT_SUM_HASURA);
  useEffect(() => {
    if (loadingFraudDataHasura) { return; }
    if (isFraudSelection) {
      setFraudChartDataHasura(tablePivotHasura(fraudDataHasura.fraud_amount_sum_frauds));
    } else {  
      setFraudChartDataHasura(tablePivotHasura(fraudDataHasura.fraud_amount_sum_non_frauds));
    }
  }, [ fraudDataHasura ]);

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
              <h3 style={{display: 'flex', justifyContent: 'center'}}>Hasura + Cube {timestamps.CubeQuery ? `(${timestamps.CubeQuery / 1000}s)` : ``}</h3>
              <DisplayFraudAmountSum
                loading={loadingFraudDataCube}
                error={errorFraudDataCube}
                chartData={fraudChartDataCube}
              />
            </div>
          </td>
          <td style={{ width: '50%' }}>
            <div style={{ height: '375px', margin: '20px 0' }}>
              <h3 style={{display: 'flex', justifyContent: 'center'}}>Hasura + PostgreSQL {timestamps.HasuraQuery ? `(${timestamps.HasuraQuery / 1000}s)` : ``}</h3>
              <DisplayFraudAmountSum
                loading={loadingFraudDataHasura}
                error={errorFraudDataHasura}
                chartData={fraudChartDataHasura}
              />
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </>
}
