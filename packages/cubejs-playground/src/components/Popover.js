import styled from 'styled-components';
import { Popover as AntdPopover } from 'antd';

const StyledPopover = styled(AntdPopover)`
  && {
    .ant-popover-inner-content {
      padding: 0;
    }
  }
`;

export default StyledPopover;
