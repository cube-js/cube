import React from 'react';
import { Row, Col, Divider } from 'antd';

const pivotConfig = {
  x: ['Users.country'],
  y: ['measures']
};

export default function PivotConfig({ pivotConfig }) {
  // return <span>{JSON.stringify(pivotConfig)}</span>;
  return (
    <>
      <Row gutter={8}>
        <Col>x</Col>
        <Col>Users.country</Col>
      </Row>

      <Row>
        <Col span={24}>
          <Divider />
        </Col>
      </Row>

      <Row gutter={8}>
        <Col>y</Col>
        <Col>measures</Col>
      </Row>
    </>
  );
}
