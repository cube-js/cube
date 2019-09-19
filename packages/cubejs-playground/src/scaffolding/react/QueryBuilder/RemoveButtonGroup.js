import React from 'react';
import * as PropTypes from 'prop-types';
import { Button } from 'antd';

const RemoveButtonGroup = ({ onRemoveClick, children, ...props }) => (
  <Button.Group style={{ marginRight: 8 }} {...props}>
    {children}
    <Button
      type="danger"
      icon="close"
      onClick={onRemoveClick}
    />
  </Button.Group>
);

RemoveButtonGroup.propTypes = {
  onRemoveClick: PropTypes.func.isRequired,
  children: PropTypes.object.isRequired
};

export default RemoveButtonGroup;
