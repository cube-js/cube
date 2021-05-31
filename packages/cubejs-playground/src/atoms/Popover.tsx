import styled from 'styled-components';
import { Popover as AntdPopover } from 'antd';

export const Popover = styled(AntdPopover)`
  && {
    .ant-popover-inner-content {
      padding: 0;
    }
  }
`;
