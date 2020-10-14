import styled from 'styled-components';
import { Tabs as AntdTabs } from 'antd';
// import vars from '../variables';

const StyledTabs = styled(AntdTabs)`
  && {
    user-select: none;
  
    .ant-tabs-nav {
      padding: 0 16px;
    }
    
    .ant-tabs-content-holder {
      padding: 0 16px;
    }
  }
`;

StyledTabs.TabPane = styled(AntdTabs.TabPane)`
  && {
        
  }
`;

export default StyledTabs;
