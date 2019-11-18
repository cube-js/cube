import React from 'react';
import styled from 'styled-components';
import { Button as AntdButton } from 'antd';

const StyledButton = styled(AntdButton)`
  border: none;
  padding: 0 27px;
  height: 40px;
`;

const Button = (props) => (
  <StyledButton {...props} />
);

export default Button;
