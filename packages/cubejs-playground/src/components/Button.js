import styled from 'styled-components';
import { Button as AntdButton } from 'antd';

const StyledButton = styled(AntdButton)`
  && {
    padding: 5px 12px;
    height: auto;
    border-color: var(--dark-05-color); 
    color: var(--text-color);
    box-shadow: none;

    &:hover, &:active, &:focus {
      border-color: var(--purple-04-color);
      color: var(--primary-color);
    }
    
    &.ant-btn-primary:not([disabled]) {
      background: var(--primary-color);
      color: white;
      border-color: var(--primary-color);
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
    background-color: var(--primary-bg);
    color: var(--primary-color);
    border: 1px solid var(--primary-color);
    
    &:not(:first-child):not(:last-child) {
      border-left-color: var(--primary-color);
      border-right-color: var(--primary-color);
    }
    
    &:first-child {
      border-right-color: var(--primary-color);
    }
    
    &:last-child {
      border-left-color: var(--primary-color);
    }
  }

  && .ant-btn-primary:not([disabled]) + .ant-btn:not(.ant-btn-primary):not([disabled]) {
    border-left-color: var(--primary-color);
    
    &:hover, &:active, &:focus {
      border-left-color: var(--primary-color);
    }
  }
`;

export default StyledButton;
