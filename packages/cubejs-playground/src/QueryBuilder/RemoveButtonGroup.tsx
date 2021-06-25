import { ReactNode } from 'react';
import styled from 'styled-components';
import { CloseOutlined } from '@ant-design/icons';

import { Button } from '../atoms';

type UnstyledRemoveButtonGroupProps = {
  onRemoveClick: () => void;
  children: ReactNode;
  disabled?: boolean;
  [k: string]: any;
};

const UnstyledRemoveButtonGroup = ({
  onRemoveClick,
  children,
  ...props
}: UnstyledRemoveButtonGroupProps) => (
  <Button.Group {...props}>
    {children}

    <Button
      ghost
      className="remove-btn"
      disabled={props.disabled}
      onClick={onRemoveClick}
    >
      <CloseOutlined />
    </Button>
  </Button.Group>
);

function color(props) {
  const colorMap = {
    primary: 'primary',
    danger: 'pink',
  };

  if (props.color == null) {
    return 'primary';
  }

  return colorMap[props.color];
}

const RemoveButtonGroup = styled(UnstyledRemoveButtonGroup)`
  && {
    border-radius: calc(var(--border-radius-base) + 1px);

    .ant-btn {
      border: none;

      & + .ant-btn {
        margin-left: 0;
      }
    }

    .remove-btn {
      padding: 8px;

      .anticon {
        height: 14px;
        display: block;
      }
    }
  }

  &&:not(.disabled) {
    border: 1px solid var(--${color}-color);
    color: var(--${color}-color);

    .ant-btn {
      background-color: var(--${color}-9);
      color: var(--${color}-color);

      span {
        color: var(--${color}-color);
      }

      &:hover {
        background-color: var(--${color}-8);
        border: none;
        box-shadow: none;
      }
    }

    .remove-btn {
      background-color: white !important;
      color: var(--${color}-color);

      &:hover {
        background-color: var(--${color}-8) !important;
      }
    }
  }

  &&.disabled {
    border: 1px solid var(--disabled-color);
    color: var(--disabled-color);

    .ant-btn {
      background-color: var(--disabled-bg);
      color: var(--disabled-color);
      border: none;

      span {
        color: var(--disabled-color);
      }
    }

    .remove-btn {
      color: var(--disabled-color);
    }
  }
`;

export default RemoveButtonGroup;
