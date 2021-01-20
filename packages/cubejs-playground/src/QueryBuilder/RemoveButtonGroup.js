import styled from 'styled-components';
import * as PropTypes from 'prop-types';
import { CloseOutlined } from '@ant-design/icons';
import { Button } from '../components';

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
  && {
    border: 1px solid var(--primary-color);
    color: var(--primary-color);
    border-radius: calc(var(--border-radius-base) + 1px);
  
    .ant-btn {
      background-color: var(--primary-9);
      color: var(--primary-color);
      border: none;
      
      span {
        color: var(--primary-color);
      }
      
      &:hover {
        background-color: var(--primary-8);
        border: none;
        box-shadow: none;
      }
      
      & + .ant-btn {
        margin-left: 0;
      }
    }

    .remove-btn {
      background-color: white !important;    
      color: var(--primary-color);
      padding: 8px;
      
      &:hover {
        background-color: var(--remove-btn-hover-bg) !important;
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
