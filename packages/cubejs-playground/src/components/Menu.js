import styled from 'styled-components';
import { Menu as AntdMenu } from 'antd';
import vars from '../variables';

const StyledMenu = styled(AntdMenu)`
  && {
    user-select: none;
    
    &.ant-menu-inline .ant-menu-item:not(:last-child) {
      margin-bottom: 0;
    }
  }
`;

StyledMenu.Item = styled(AntdMenu.Item)`
  &&& {
    border-radius: 4px;
    margin-bottom: 0;
    margin-top: 0;
    
    &::after {
      border-color: transparent;
    }
  
    &.ant-menu-item-selected {
      color: ${vars.primaryColor};
    
      &::after {
        border-color: transparent;
      }
    }
  }
`;

export default StyledMenu;
