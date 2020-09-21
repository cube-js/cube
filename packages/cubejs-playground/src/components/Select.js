import styled from 'styled-components';
import { Select as AntdSelect } from 'antd';
import vars from '../variables';

const StyledSelect = styled(AntdSelect)`
  &&& {
    &.ant-select-single {
      .ant-select-selector {
        padding: 5px 12px;
      }
    }
    &.ant-select-multiple {
      .ant-select-selector {
        padding: 2px 12px;      
      }    
    }
    
    .ant-select-selection-item {
      font-size: 14px;
      line-height: 22px;
    }
      
    .ant-select-selector {  
      height: auto;
      border-color: 1px solid ${vars.dark05Color};
      color: ${vars.textColor};
      font-size: 14px;
      line-height: 22px;
      
      &:hover, &:active, &:focus {
        border-color: ${vars.purple04Color};
        color: ${vars.primaryColor};
      }    
    }
    
    .ant-select-selector::after {
      font-size: 14px;
      line-height: 22px;
    }
  }
`;

export default StyledSelect;
