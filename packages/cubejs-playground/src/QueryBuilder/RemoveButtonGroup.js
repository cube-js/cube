import React from 'react';
import styled from 'styled-components';
import * as PropTypes from 'prop-types';
import { CloseOutlined } from '@ant-design/icons';
import { Button } from '../components';
import vars from '../variables';

const RemoveButtonGroup = ({ onRemoveClick, children, ...props }) => (
  <Button.Group {...props}>
    {children}
    <Button ghost onClick={onRemoveClick} className="remove-btn">
      <CloseOutlined />
    </Button>
  </Button.Group>
);

RemoveButtonGroup.propTypes = {
  onRemoveClick: PropTypes.func.isRequired,
  children: PropTypes.object.isRequired,
};

const styledRemoveButtonGroup = styled(RemoveButtonGroup)`
  &.ant-btn-group {
    border: 1px solid ${vars.primaryColor};
    color: ${vars.primaryColor};
    border-radius: calc(${vars.borderRadiusBase} + 1px);
  
    .ant-btn {
      background-color: ${vars.removeBtnBg};
      color: ${vars.primaryColor};
      border: none;
      
      &:hover {
        background-color: ${vars.removeBtnHoverBg};
        border: none;
        box-shadow: none;
      }
      
      & + .ant-btn {
        margin-left: 0;
      }
    }
    
    .remove-btn {
      background-color: white !important;    
      color: ${vars.primaryColor};
      padding: 8px;
      
      &:hover {
        background-color: ${vars.removeBtnHoverBg} !important;
      }
      
      .anticon {
        height: 14px;
        display: block;
        vertical-align: initial;
      }
    }
  }
`;

export default styledRemoveButtonGroup;
