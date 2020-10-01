import React from 'react';
import { Row, Col } from 'antd';
import SchemaCube from './SchemaCube';
import Query from './Query';

export default () => {
  return (
    <React.Fragment>
      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={12}>
          <div className='dashboard__cell'>
            <Query />
          </div>
        </Col>
        <Col sm={24} lg={12}>
          <div className='dashboard__cell'>
            <SchemaCube />
          </div>
        </Col>
      </Row>
    </React.Fragment>
  );
};
