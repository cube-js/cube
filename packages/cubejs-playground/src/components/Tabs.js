import styled from 'styled-components';
import { Tabs as AntdTabs } from 'antd';

const StyledTabs = styled(AntdTabs)`
  && {
    .ant-tabs-nav {
      padding: 0 16px;
      user-select: none;
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
