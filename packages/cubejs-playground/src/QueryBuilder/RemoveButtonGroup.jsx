import React from 'react';
import * as PropTypes from 'prop-types';
import { CloseOutlined } from '@ant-design/icons';
import { Button } from 'antd';

const RemoveButtonGroup = ({ onRemoveClick, children, ...props }) => (
  <Button.Group style={{ marginRight: 8 }} {...props}>
    {children}
    <Button danger ghost onClick={onRemoveClick}>
      <CloseOutlined />
    </Button>
  </Button.Group>
);

RemoveButtonGroup.propTypes = {
  onRemoveClick: PropTypes.func.isRequired,
  children: PropTypes.object.isRequired,
};

export default RemoveButtonGroup;
