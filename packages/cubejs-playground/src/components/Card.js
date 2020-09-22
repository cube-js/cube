import styled from 'styled-components';
import { Card as AntdCard } from 'antd';
// import vars from '../variables';

const StyledCard = styled(AntdCard)`
  && {
    .ant-card-head {
      border-bottom: 2px solid #D7D7F488;
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
  }
`;

export default StyledCard;
