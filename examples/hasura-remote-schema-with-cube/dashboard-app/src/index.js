import React from 'react';
import { useState, useEffect } from 'react'
import { Col, Container, Form, Row } from 'react-bootstrap';
import 'bootstrap/dist/css/bootstrap.min.css';
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
import _ from 'lodash';
import LineChart from './components/LineChart';
import LoadingIndicator from './components/LoadingIndicator'

function range(start, end) {
  const arr = [...Array(end - start + 1).keys()].map(x => x + start)
  return arr;
}

function tablePivotCube(data) {
  return _.reduce(_.mapValues(
    _.groupBy(data.cube.map(({ fraud: { amountSum, step, type } }) => ({ y: amountSum, x: step, type })), 'type'),
    list => list.map(fraud => _.omit(fraud, 'type'))
  ), (accumulator, iterator, key) => {
    accumulator.push({
      id: key,
      data: iterator
    });

    return accumulator;
  }, [])
}

function tablePivotHasura(data) {
  return _.reduce(_.mapValues(
    _.groupBy(data.map(({ fraud__amount_sum, fraud__step, fraud__type }) => ({ y: fraud__amount_sum, x: fraud__step, type: fraud__type })), 'type'),
    list => list.map(fraud => _.omit(fraud, 'type'))
  ), (accumulator, iterator, key) => {
    accumulator.push({
      id: key,
      data: iterator
    });

    return accumulator;
  }, [])
}

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
const client = new ApolloClient({
  link: authLink.concat(httpLink),
  cache: new InMemoryCache()
});

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(
    <ApolloProvider client={client}>
      <App />
    </ApolloProvider>,
  )

const availableStepRanges = [
  { id: 1, start: 1, end: 50 },
  { id: 2, start: 50, end: 100 },
  { id: 3, start: 100, end: 150 },
  { id: 4, start: 150, end: 200 },
  { id: 5, start: 200, end: 250 },
  { id: 6, start: 250, end: 300 },
  { id: 7, start: 300, end: 350 },
  { id: 8, start: 400, end: 450 },
  { id: 9, start: 450, end: 500 },
  { id: 10, start: 500, end: 550 },
  { id: 11, start: 550, end: 600 },
  { id: 12, start: 600, end: 650 },
  { id: 13, start: 650, end: 700 },
  { id: 14, start: 700, end: 750 },
];

const defaultStepSelection = 1;
const defaultIsFraudSelection = 1;

function App() {
  const [ fraudChartDataCube, setFraudChartDataCube ] = useState([])
  const [ fraudChartDataHasura, setFraudChartDataHasura ] = useState([])

  const [ stepSelection, setStepSelection ] = useState(defaultStepSelection);
  const selectedStep = availableStepRanges.find(x => x.id === stepSelection);
  const selectedStepRange = range(selectedStep.start, selectedStep.end);

  const [ isFraudSelection, setIsFraudSelection ] = useState(defaultIsFraudSelection);

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
  const { loading: loadingFraudDataCube, error: errorFraudDataCube, data: fraudDataCube } = useQuery(GET_FRAUD_AMOUNT_SUM_CUBE_REMOTE_SCHEMA);
  useEffect(() => {
    if (fraudDataCube) {
      setFraudChartDataCube(
        tablePivotCube(fraudDataCube)
      )
    }
  }, [ fraudDataCube ])
  function DisplayFraudAmountSumCube() {
    if (loadingFraudDataCube) return <LoadingIndicator />;

    if (errorFraudDataCube) {
      console.error(errorFraudDataCube);
      return <p>Error :( </p>;
    }

    return (
      <LineChart
        data={fraudChartDataCube}
      />
    );
  }

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

  const { loading: loadingFraudChartDataHasura, error: errorFraudChartDataHasura, data: fraudDataHasura } = useQuery(GET_FRAUD_AMOUNT_SUM_HASURA);
  useEffect(() => {
    if (fraudDataHasura) {
      if (isFraudSelection) {
        setFraudChartDataHasura(
          tablePivotHasura(fraudDataHasura.fraud_amount_sum_frauds)
        )
      } else {  
        setFraudChartDataHasura(
          tablePivotHasura(fraudDataHasura.fraud_amount_sum_non_frauds)
        )
      }
    }
  }, [ fraudDataHasura ])

  
  function DisplayFraudAmountSumHasura() {
    if (loadingFraudChartDataHasura) return <LoadingIndicator />;

    if (errorFraudChartDataHasura) {
      console.error(errorFraudChartDataHasura);
      return <p>Error :( </p>;
    }

    return (
      <LineChart
        data={fraudChartDataHasura}
      />
    );
  }

  return <>
    <Container>
      <Row className='mb-12'>
        <Form>
          <Form.Group className='mb-3' as={Row}>
            <Form.Label column sm={{ span: 2, offset: 4 }}>Transaction step in time</Form.Label>
            <Col sm={{ span: 2 }}>
              <Form.Select
                value={stepSelection}
                onChange={e => setStepSelection(parseInt(e.target.value))}
              >
                {availableStepRanges.map(stepRange => (
                  <option key={stepRange.id} value={stepRange.id}>
                    Start: {stepRange.start}, End: {stepRange.end}
                  </option>
                ))}
              </Form.Select>
            </Col>
          </Form.Group>
        </Form>
      </Row>
      <Row className='mb-12'>
        <Form>
          <Form.Group className='mb-3' as={Row}>
            <Form.Label column sm={{ span: 2, offset: 4 }}>Is a fraudulent transaction</Form.Label>
            <Col sm={{ span: 2 }}>
              <Form.Select
                value={isFraudSelection}
                onChange={e => setIsFraudSelection(parseInt(e.target.value))}
              >
                <option key={0} value={0}>
                  No
                </option>
                <option key={1} value={1}>
                  Yes
                </option>
              </Form.Select>
            </Col>
          </Form.Group>
        </Form>
      </Row>
      <Row className='mb-12' style={{ height: '400px', margin: '50px 0' }}>
        <Col md={{ span: 6 }} style={{ height: '400px', margin: '0px' }}>
          <h3 style={{display: 'flex', justifyContent: 'center'}}>Cube</h3>
          <DisplayFraudAmountSumCube />
        </Col>
        <Col md={{ span: 6 }} style={{ height: '400px', margin: '0px' }}>         
          <h3 style={{display: 'flex', justifyContent: 'center'}}>Hasura</h3>
          <DisplayFraudAmountSumHasura />
        </Col>
      </Row>
    </Container>
  </>
}