import styled from 'styled-components';
import { Card as AntdCard } from 'antd';

const StyledCard = styled(AntdCard)`
  && {
    border-radius: 8px;
    border: none;
  
    .ant-card-head {
      border-bottom: 1px solid #D7D7F488;
      padding: 8px 16px;
    }
    
    .ant-card-head-title {
      padding: 0;
      flex: initial;
    }
    
    .ant-card-extra {
      padding: 0;
    }
    
    .ant-card-head-wrapper {
      flex-flow: row wrap;
      place-content: space-between;
    }
    
    .ant-card-head-title {
      font-size: 18px;
      line-height: 40px;
    }
    
    .ant-card-body {
      padding: 16px;
    }
  }
`;

export default StyledCard;
