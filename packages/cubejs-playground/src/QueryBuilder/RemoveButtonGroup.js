import styled from 'styled-components';
import { CloseOutlined } from '@ant-design/icons';

import { Button } from '../components';

const UnstyledRemoveButtonGroup = ({ onRemoveClick, children, ...props }) => (
  <Button.Group {...props}>
    {children}
    <Button ghost onClick={onRemoveClick} className="remove-btn">
      <CloseOutlined />
    </Button>
  </Button.Group>
);

function color(props) {
  const colorMap = {
    primary: 'primary',
    danger: 'pink'
  };
  
  if (props.color == null) {
    return 'primary';
  }
  
  return colorMap[props.color];
}

const RemoveButtonGroup = styled(UnstyledRemoveButtonGroup)`
  && {
    border: 1px solid var(--${color}-color);
    color: var(--${color}-color);
    border-radius: calc(var(--border-radius-base) + 1px);

    .ant-btn {
      background-color: var(--${color}-9);
      color: var(--${color}-color);
      border: none;

      span {
        color: var(--${color}-color);
      }

      &:hover {
        background-color: var(--${color}-8);
        border: none;
        box-shadow: none;
      }

      & + .ant-btn {
        margin-left: 0;
      }
    }

    .remove-btn {
      background-color: white !important;
      color: var(--${color}-color);
      padding: 8px;

      &:hover {
        background-color: var(--${color}-8) !important;
      }

      .anticon {
        height: 14px;
        display: block;
        vertical-align: initial;
      }
    }
  }
`;

export default RemoveButtonGroup;
