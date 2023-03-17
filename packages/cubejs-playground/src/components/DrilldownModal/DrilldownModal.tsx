import React, { CSSProperties } from 'react';
import { Button, Modal } from 'antd';
import { useCubeQuery } from '@cubejs-client/react';

import { FatalError } from '../../components/Error/FatalError';
import { CubeLoader } from '../../atoms';

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
  };

  return (
    <Modal
      style={modalStyle}
      visible={isOpen}
      onCancel={handleCancel}
      width="auto"
      footer={
        <Button key="close" onClick={handleCancel}>
          Close
        </Button>
      }
      afterClose={onClose}
      centered
    >
      {error ? <FatalError error={error} /> : null}
      {isLoading && !error ? <CubeLoader /> : null}
      {resultSet && !isLoading ? (
        <TableQueryRenderer resultSet={resultSet} pivotConfig={pivotConfig} />
      ) : null}
    </Modal>
  );
}
