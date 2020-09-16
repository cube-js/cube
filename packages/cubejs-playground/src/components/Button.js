// import React from 'react';
import styled from 'styled-components';
import { Button as AntdButton } from 'antd';
import vars from '../variables';

const StyledButton = styled(AntdButton)`
  &.ant-btn {
    padding: 5px 12px;
    height: auto;
    border: 1px solid ${vars.dark05};
    color: ${vars.textColor};
    
    &.ant-btn-dashed {
      border-style: dashed;
    }
    
    &:hover {
      border: 1px solid ${vars.purple04};
      color: ${vars.primaryColor};
    }
    
    &:active {
      border: 1px solid ${vars.purple04};
      color: ${vars.primaryColor};
    }
    
    &.ant-btn-icon-only {
      display: inline-flex;
      place-items: center;
      padding: 5px 8px;
    }
    
    .anticon {
      display: inline-block;
      height: 14px;
    }
  }
`;

export default StyledButton;
