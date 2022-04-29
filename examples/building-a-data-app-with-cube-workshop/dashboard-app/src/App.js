import cubejs from '@cubejs-client/core';
import './App.css';
import { useEffect, useState } from 'react';
import { Accordion, Col, Container, Form, Row } from 'react-bootstrap';
import jwt from 'jsonwebtoken';
import { CustomBarChart } from './CustomBarChart';
import { CustomPieChart } from './CustomPieChart';

const defaultJwtSecret = 'SECRET';
const defaultApiUrl = 'http://localhost:4000/cubejs-api/v1';
const defaultSupplierId = 1;

function App() {
  const [ status, setStatus ] = useState(undefined);
  const [ jwtSecret, setJwtSecret ] = useState(defaultJwtSecret);
  const [ apiUrl, setApiUrl ] = useState(defaultApiUrl);

  const suppliers = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]
    .map(id => ({
      id,
      token: !jwtSecret ? 'foobar' : jwt.sign({
        exp: 5000000000,
        supplierId: id,
      }, jwtSecret),
    }));

  const [ supplierId, setSupplierId ] = useState(defaultSupplierId);
  const supplier = suppliers.find(x => x.id === supplierId);

  const cubejsApi = cubejs(
    supplier.token,
    { apiUrl },
  );

  useEffect(() => {
    cubejsApi
      .meta()
      .then(() => setStatus(true))
      .catch(() => setStatus(false));
  }, [
    supplier.token,
    apiUrl,
  ]);

  const [ ordersBarData, setOrdersBarData ] = useState({});

  useEffect(() => {
    cubejsApi
      .load({
        measures: [ 'Orders.count' ],
        dimensions: [ 'Orders.status' ],
        timeDimensions: [ {
          dimension: 'Orders.createdAt',
          granularity: 'month',
        } ],
      })
      .then(data => {
        setOrdersBarData(data)
      })
      .catch(() => setStatus(false));
  }, [
    supplier.token,
    apiUrl,
  ]);

  const [ ordersPieData, setOrdersPieData ] = useState({});

  useEffect(() => {
    cubejsApi
      .load({
        measures: [ 'Orders.count' ],
        dimensions: [ 'Orders.status' ],
        timeDimensions: [],
      })
      .then(data => {
        setOrdersPieData(data)
      })
      .catch(() => setStatus(false));
  }, [
    supplier.token,
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
                      value={supplier.token}
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
            <Form.Label column sm={2}>Supplier</Form.Label>
            <Col sm={10}>
              <Form.Select
                value={supplierId}
                onChange={e => setSupplierId(parseInt(e.target.value))}
              >
                {suppliers.map(supplier => (
                  <option key={supplier.id} value={supplier.id}>
                    {supplier.id}
                  </option>
                ))}
              </Form.Select>
            </Col>
          </Form.Group>
        </Form>
      </Row>
      <Row className='mb-3' style={{ height: 300 }}>
        <CustomBarChart
          data={ordersBarData}
        />
      </Row>
      <Row className='mb-3' style={{ height: 300 }}>
        <CustomPieChart
          data={ordersPieData}
        />
      </Row>
    </Container>
  );
}

export default App;
