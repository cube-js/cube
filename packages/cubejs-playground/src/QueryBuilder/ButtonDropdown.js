import React from 'react';
import * as PropTypes from 'prop-types';
import { Dropdown } from 'antd';
import { Button } from '../components';
// import styled from 'styled-components';
// import vars from '../variables';

const ButtonDropdown = ({ overlay, ...buttonProps }) => (
  <Dropdown
    overlay={overlay} placement="bottomLeft" trigger={['click']}>
    <Button {...buttonProps} />
  </Dropdown>
);

ButtonDropdown.propTypes = {
  overlay: PropTypes.object.isRequired,
};

// const styledButtonDropdown = styled(ButtonDropdown)`
//   &.ant-btn {
//     border-style: dashed;
//   }
// `;
//
// export default styledButtonDropdown;

export default ButtonDropdown;
