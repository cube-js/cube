import cubejs from '@cubejs-client/core';
import './App.css';
import { useEffect, useState } from 'react';
import { Accordion, Col, Container, Form, Row } from 'react-bootstrap';
import jwt from 'jsonwebtoken';
import { LineChart } from './LineChart';
import { TreeMapChart } from './TreeMapChart';

const defaultJwtSecret = 'SECRET';
const defaultApiUrl = 'https://fashionable-pike.aws-us-east-2.cubecloudapp.dev/cubejs-api/v1';
const defaultMerchantId = 5;

function App() {
  const [ status, setStatus ] = useState(undefined);
  const [ jwtSecret, setJwtSecret ] = useState(defaultJwtSecret);
  const [ apiUrl, setApiUrl ] = useState(defaultApiUrl);

  const merchants = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]
    .map(id => ({
      id,
      token: !jwtSecret ? 'foobar' : jwt.sign({
        exp: 5000000000,
        merchant_id: id,
      }, jwtSecret),
    }));

  const [ merchantId, setMerchantId ] = useState(defaultMerchantId);
  const merchant = merchants.find(x => x.id === merchantId);

  const cubejsApi = cubejs(
    merchant.token,
    { apiUrl },
  );

  useEffect(() => {
    cubejsApi
      .meta()
      .then(() => setStatus(true))
      .catch(() => setStatus(false));
  }, [
    cubejsApi,
    merchant.token,
    apiUrl,
  ]);

  const [ ordersData, setOrdersData ] = useState([]);

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
      .then(data => setOrdersData(data.tablePivot()))
      .catch(() => setStatus(false));
  }, [
    cubejsApi,
    merchant.token,
    apiUrl,
  ]);

  const [ categoriesData, setCategoriesData ] = useState([]);

  useEffect(() => {
    cubejsApi
      .load({
        measures: [ 'Orders.count' ],
        dimensions: [ 'ProductCategories.name' ],
      })
      .then(data => setCategoriesData(data.tablePivot()))
      .catch(() => {
      });
  }, [
    cubejsApi,
    merchant.token,
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
                      value={merchant.token}
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
            <Form.Label column sm={2}>Merchant</Form.Label>
            <Col sm={10}>
              <Form.Select
                value={merchantId}
                onChange={e => setMerchantId(parseInt(e.target.value))}
              >
                {merchants.map(merchant => (
                  <option key={merchant.id} value={merchant.id}>
                    {merchant.id}
                  </option>
                ))}
              </Form.Select>
            </Col>
          </Form.Group>
        </Form>
      </Row>
      <Row className='mb-3' style={{ height: 300 }}>
        {ordersData.length > 0 && (
          <LineChart
            data={ordersData}
            group={row => row['Orders.status']}
            x={row => row['Orders.createdAt.month'].split('T')[0]}
            y={row => parseInt(row['Orders.count'])}
          />
        )}
      </Row>
      <Row style={{ height: 200 }}>
        {categoriesData.length > 0 && (
          <TreeMapChart
            data={categoriesData}
            name={row => row['ProductCategories.name']}
            value={row => parseInt(row['Orders.count'])}
          />
        )}
      </Row>
    </Container>
  );
}

export default App;
