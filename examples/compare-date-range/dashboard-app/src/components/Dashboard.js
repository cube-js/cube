import React from 'react';
import { Row, Col } from 'antd';
import SingleQuery from './SingleQuery';
import MultipleQueries from './MultipleQueries';

export default () => {
  return (
    <React.Fragment>
      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={12}>
          <div className='dashboard__cell'>
            <MultipleQueries />
          </div>
        </Col>
        <Col sm={24} lg={12}>
          <div className='dashboard__cell'>
            <SingleQuery />
          </div>
        </Col>
      </Row>
    </React.Fragment>
  );
};
