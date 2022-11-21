import React from 'react';
import * as PropTypes from 'prop-types';
import { Button } from 'antd';
import { Icon } from '@ant-design/compatible';

const RemoveButtonGroup = ({ onRemoveClick, children, ...props }) => (
  <Button.Group
    style={{
      marginRight: 8,
    }}
    {...props}
  >
    {children}
    <Button type="danger" ghost onClick={onRemoveClick}>
      <Icon type="close" />
    </Button>
  </Button.Group>
);

RemoveButtonGroup.propTypes = {
  onRemoveClick: PropTypes.func.isRequired,
  children: PropTypes.object.isRequired,
};
export default RemoveButtonGroup;
