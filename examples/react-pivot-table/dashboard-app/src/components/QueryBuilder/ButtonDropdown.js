import React from 'react';
import * as PropTypes from 'prop-types';
import { Button, Dropdown } from 'antd';

const ButtonDropdown = ({ overlay, ...buttonProps }) => (
  <Dropdown overlay={overlay} placement="bottomLeft" trigger={['click']}>
    <Button {...buttonProps} />
  </Dropdown>
);

ButtonDropdown.propTypes = {
  overlay: PropTypes.object.isRequired,
};
export default ButtonDropdown;
