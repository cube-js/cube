import React from 'react';
import { Col } from 'antd';

const MobileSearch: React.FC = (props) => (
  <Col md={0} xs={24}>
    {props.children}
  </Col>
);

export default MobileSearch;
