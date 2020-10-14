import styled from 'styled-components';
import { Button as AntdButton } from 'antd';
import vars from '../variables';

const StyledButton = styled(AntdButton)`
  && {
    padding: 5px 12px;
    height: auto;
    border-color: ${vars.dark05Color}; 
    color: ${vars.textColor};
    box-shadow: none;

    &:hover, &:active, &:focus {
      border-color: ${vars.purple04Color};
      color: ${vars.primaryColor};
    }
    
    &.ant-btn-primary:not([disabled]) {
      background: ${vars.primaryColor};
      color: white;
      border-color: ${vars.primaryColor};
      place-self: center;
    }
    
    &.ant-btn-icon-only {
      display: inline-flex;
      place-items: center;
      padding: 5px 8px;
      font-size: 14px;
      
      svg {
        width: 15px;
        height: 14px;
      }
    }
    
    .anticon {
      display: inline-block;
      height: 14px;
    }
    
    &.ant-btn-sm {
      padding: 0 8px;
    }
  }
`;

StyledButton.Group = styled(AntdButton.Group)`
  &&& .ant-btn-primary:not([disabled]) {
    background-color: ${vars.primaryBg};
    color: ${vars.primaryColor};
    border: 1px solid ${vars.primaryColor};
    
    &:not(:first-child):not(:last-child) {
      border-left-color: ${vars.primaryColor};
      border-right-color: ${vars.primaryColor};
    }
    
    &:first-child {
      border-right-color: ${vars.primaryColor};
    }
    
    &:last-child {
      border-left-color: ${vars.primaryColor};
    }
  }

  && .ant-btn-primary:not([disabled]) + .ant-btn:not(.ant-btn-primary):not([disabled]) {
    border-left-color: ${vars.primaryColor};
    
    &:hover, &:active, &:focus {
      border-left-color: ${vars.primaryColor};
    }
  }
`;

export default StyledButton;
