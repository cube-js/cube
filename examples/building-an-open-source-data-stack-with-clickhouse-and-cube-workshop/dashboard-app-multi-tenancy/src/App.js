import cubejs from '@cubejs-client/core';
import './App.css';
import { useEffect, useState } from 'react';
import { Accordion, Col, Container, Form, Row } from 'react-bootstrap';
import jwt from 'jsonwebtoken';
import { CustomBarChart } from './CustomBarChart';

const defaultJwtSecret = '1c7548fdc11622f711fd0113139feefc4cbd88826d3107b29b4950b0b1df159c';
const defaultDataSourceId = 1;

/** OSS Cube */
// const defaultApiUrl = 'http://localhost:4000/cubejs-api/v1';
/** Cube Cloud */
const defaultApiUrl = 'https://blue-stork.aws-us-east-1.cubecloudapp.dev/cubejs-api/v1';

function App() {
  const [ timer, setTimer ] = useState({});
  const [ status, setStatus ] = useState(undefined);
  const [ jwtSecret, setJwtSecret ] = useState(defaultJwtSecret);
  const [ apiUrl, setApiUrl ] = useState(defaultApiUrl);

  const dataSources = [{ id: 1, dataSource: 'ClickHouse' }, { id: 2, dataSource: 'MySQL' }]
    .map(({ id, dataSource }) => ({
      id,
      dataSource,
      token: !jwtSecret ? 'foobar' : jwt.sign({
        exp: 5000000000,
        dataSource: dataSource.toLowerCase(),
      }, jwtSecret),
    }));

  const [ dataSourceId, setDataSourceId ] = useState(defaultDataSourceId);
  const dataSource = dataSources.find(x => x.id === dataSourceId);

  const cubejsApi = cubejs(
    dataSource.token,
    { apiUrl },
  );

  useEffect(() => {
    cubejsApi
      .meta()
      .then(() => setStatus(true))
      .catch(() => setStatus(false));
  }, [
    dataSource.token,
    apiUrl,
  ]);

  useEffect(() => {
    setTimer({});
    const start = Date.now();

    cubejsApi
      .meta()
      .then(() => {
        const end = Date.now();
        const responseTime = end - start;
        setTimer({ responseTime });
      });
  }, [
    dataSource.token,
    apiUrl,
  ]);

  const [ ontimeBarData, setOntimeBarData ] = useState({});
  useEffect(() => {
    cubejsApi
      .load({
        measures: [ 'Ontime.avgDepDelayGreaterThanTenMinutesPercentage' ],
        dimensions: [],
        timeDimensions: [ {
          dimension: 'Ontime.flightdate',
          granularity: 'year',
        } ],
      })
      .then(data => {
        setOntimeBarData(data)
      })
      .catch(() => setStatus(false));
  }, [
    dataSource.token,
    apiUrl,
  ]);

  return (
    <Container>
      <Row className='mb-3'>
        <Accordion>
          <Accordion.Item eventKey='0'>
            <Accordion.Header>Cube
              API {status === undefined ? '' : status ? 'is ready' : 'is not available'}</Accordion.Header>
            <Accordion.Body>
              <Form>
                <Form.Group className='mb-3' as={Row}>
                  <Form.Label column sm={2}>URL</Form.Label>
                  <Col sm={10}>
                    <Form.Control
                      type='text'
                      placeholder='URL'
                      value={apiUrl}
                      onChange={e => setApiUrl(e.target.value)}
                    />
                  </Col>
                </Form.Group>
                <Form.Group className='mb-3' as={Row}>
                  <Form.Label column sm={2}>JWT secret</Form.Label>
                  <Col sm={10}>
                    <Form.Control
                      type='text'
                      placeholder=''
                      value={jwtSecret}
                      onChange={e => setJwtSecret(e.target.value)}
                    />
                  </Col>
                </Form.Group>
                <Form.Group className='mb-3' as={Row}>
                  <Form.Label column sm={2}>JWT</Form.Label>
                  <Col sm={10}>
                    <Form.Control
                      className='mb-2'
                      as='textarea'
                      value={dataSource.token}
                      readOnly={true}
                    />
                    <div>You can decode the token at <a href='https://jwt.io' target='_blank'
                                                        rel='noreferrer'>jwt.io</a></div>
                  </Col>
                </Form.Group>
              </Form>
            </Accordion.Body>
          </Accordion.Item>
        </Accordion>
      </Row>
      <Row className='mb-3'>
        <Form>
          <Form.Group className='mb-3' as={Row}>
            <Col sm={12}>
              <Form.Select
                value={dataSourceId}
                onChange={e => setDataSourceId(parseInt(e.target.value))}
              >
                {dataSources.map(dataSource => (
                  <option key={dataSource.id} value={dataSource.id}>
                    {dataSource.dataSource}
                  </option>
                ))}
              </Form.Select>
            </Col>
          </Form.Group>
        </Form>
      </Row>
      <Row className='mb-3' style={{ height: 300 }}>
        <Col sm={12}>
          <p className='text-center'>{ timer.responseTime/1000 || '...' } seconds</p>
          <CustomBarChart
            data={ontimeBarData}
          />
        </Col>
      </Row>
    </Container>
  );
}

export default App;
