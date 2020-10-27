import styled from 'styled-components';
import { Alert as AntdAlert } from 'antd';

const TYPES = {
  error: {
    border: '#FFCCC7',
    background: '#FFF1F0',
  },
  warning: {
    border: '#FFE58F',
    background: '#FFFBE6',
  },
  info: {
    border: '#91D5FF',
    background: '#E6F7FF',
  },
  success :{
    border: '#B7EB8F',
    background: '#F6FFED',
  },
}

const StyledAlert = styled(AntdAlert)`
  && {
    background: ${props => TYPES[props.type || 'info'].background};
    border: 1px solid ${props => TYPES[props.type || 'info'].border};
    box-sizing: border-box;
    border-radius: 2px;
  }
`;

export default StyledAlert;
