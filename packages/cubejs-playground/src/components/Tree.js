import styled from 'styled-components';
import { Tree as AntdTree } from 'antd';

const StyledTree = styled(AntdTree)`
  && {
    user-select: none;
    
    .ant-tree-node-content-wrapper.ant-tree-node-selected {
      color: white;
    }
  }
`;

StyledTree.TreeNode = styled(AntdTree.TreeNode)`
  && {
    
  }
`;

export default StyledTree;
