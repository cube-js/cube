import React, { CSSProperties, useLayoutEffect } from 'react';
import { Button, Col, Modal, Row, Spin } from 'antd';
import { Query } from '@cubejs-client/core';
import { useCubeQuery } from '@cubejs-client/react';
import { FatalError } from '../../atoms';
import { TableQueryRenderer } from './TableQueryRenderer';

const modalStyle: CSSProperties = {
  top: 50,
  minWidth: 450,
};

export function DrilldownModal({ query, onClose, pivotConfig }) {
  const [isOpen, setIsOpen] = React.useState(true);
  const { resultSet, isLoading, error } = useCubeQuery(query, {
    skip: !query,
  });

  const handleCancel = () => {
    setIsOpen(false);
    onClose();
  };

  return (
    <Modal
      style={modalStyle}
      visible={isOpen}
      onCancel={handleCancel}
      width="auto"
      footer={[
        <Button key="close" onClick={handleCancel}>
          Close
        </Button>,
      ]}
      centered
    >
      {error ? <FatalError error={error} /> : null}
      {isLoading && !error ? (
        <Row justify="center">
          <Col>
            <Spin />
          </Col>
        </Row>
      ) : null}
      {resultSet && !isLoading ? (
        <TableQueryRenderer resultSet={resultSet} pivotConfig={pivotConfig} />
      ) : null}
    </Modal>
  );
}
