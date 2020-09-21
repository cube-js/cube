import styled from 'styled-components';
import { Input as AntdInput } from 'antd';
import vars from '../variables';

const StyledInput = styled(AntdInput)`
  && .ant-select-selector {
    padding: 5px 12px;
    height: auto;
    border-color: 1px solid ${vars.dark05Color};
    color: ${vars.textColor};
    
    &:hover, &:active, &:focus {
      border-color: ${vars.purple04Color};
      color: ${vars.primaryColor};
    }
  }
`;

export default StyledInput;
