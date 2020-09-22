import styled from 'styled-components';
import { Card as AntdCard } from 'antd';
// import vars from '../variables';

const StyledCard = styled(AntdCard)`
  && {
    .ant-card-head {
      border-bottom: 2px solid #D7D7F488;
      padding: 0 16px;          
    }
    
    .ant-card-head-title {
      font-size: 18px;
      line-height: 35px;
    }
  }
`;

export default StyledCard;
