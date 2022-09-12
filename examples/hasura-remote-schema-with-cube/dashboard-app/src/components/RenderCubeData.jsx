import React from 'react';
import { useState, useEffect } from 'react';
import * as classes from '../index.module.css';
import * as ReactDOM from 'react-dom/client';
import {
  ApolloClient,
  InMemoryCache,
  ApolloProvider,
  useQuery,
  gql,
  createHttpLink,
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
} from '../utils/utils';

const [ fraudChartDataCube, setFraudChartDataCube ] = useState([])
const [ cubeResponseTimeInMs, setCubeResponseTimeInMs ] = useState(0);

const sendDate = (new Date()).getTime();
const cubeResponseTime = {};

// Cube GQL
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

// Cube
const {
  loading: loadingFraudDataCube,
  error: errorFraudDataCube,
  data: fraudDataCube,
} = useQuery(GET_FRAUD_AMOUNT_SUM_CUBE_REMOTE_SCHEMA, {
  onCompleted: () => {
    cubeResponseTime.receiveDate = (new Date()).getTime();
    cubeResponseTime.responseTimeMs = cubeResponseTime.receiveDate - sendDate;
    setCubeResponseTimeInMs(cubeResponseTime.responseTimeMs)
    console.log('Send date: ' + sendDate);
    console.log('Cube receive date: ' + cubeResponseTime.receiveDate);
    console.log('Cube response time: ' + cubeResponseTime.responseTimeMs);
  },
});

useEffect(() => {
  if (loadingFraudDataCube) {
    return;
  }
  setFraudChartDataCube(
    tablePivotCube(fraudDataCube)
  )
}, [ fraudDataCube ])

const RenderCubeData = () => (
  <div style={{ height: '400px', margin: '10px 0' }}>
    <h3 style={{display: 'flex', justifyContent: 'center'}}>Hasura + Cube {cubeResponseTimeInMs ? `responded in ${cubeResponseTimeInMs / 1000}s` : ``}</h3>
    <DisplayFraudAmountSum
      loading={loadingFraudDataCube}
      error={errorFraudDataCube}
      chartData={fraudChartDataCube}
    />
  </div>
);

export default RenderCubeData;
