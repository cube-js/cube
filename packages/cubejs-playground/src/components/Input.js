import styled from 'styled-components';
import { Input as AntdInput } from 'antd';

const StyledInput = styled(AntdInput)`
  && .ant-select-selector {
    padding: 5px 12px;
    height: auto;
    border-color: 1px solid var(--dark-05-color);
    color: var(--text-color);
    
    &:hover, &:active, &:focus {
      border-color: var(--purple-04-color);
      color: var(--primary-color);
    }
  }
`;

export default StyledInput;
