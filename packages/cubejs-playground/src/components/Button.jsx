import React from 'react';
import styled from 'styled-components';
import { Button as AntdButton } from 'antd';

const StyledButton = styled(AntdButton)`
  padding: 0 27px;
  height: 40px;
  border-radius: 4px;
  border: 1px solid #d0d0da;
  &.ant-btn-primary {
    border: none;
    &:hover {
      background-color: #644aff;
    }
  }
`;

const Button = (props) => <StyledButton {...props} />;

export default Button;
