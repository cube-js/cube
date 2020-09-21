import styled from 'styled-components';
import { Button as AntdButton } from 'antd';
import vars from '../variables';

const StyledButton = styled(AntdButton)`
  && {
    padding: 5px 12px;
    height: auto;
    border-color: 1px solid ${vars.dark05Color};
    color: ${vars.textColor};
    margin: unset;

    &:hover, &:active, &:focus {
      border-color: ${vars.purple04Color};
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
