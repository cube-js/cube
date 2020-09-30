import React from 'react';
import styled from 'styled-components';
import { Alert as AntdAlert } from 'antd';
// import vars from '../variables';

const AlertContainer = styled.div`
  padding: 24px;
`;
const StyledAlert = styled(AntdAlert)`
  && {
    background: #E6F7FF;
    border: 1px solid #91D5FF;
    box-sizing: border-box;
    border-radius: 2px;
  }
`;

export default (props) => (
  <AlertContainer>
    <StyledAlert {...props} />
  </AlertContainer>
);
