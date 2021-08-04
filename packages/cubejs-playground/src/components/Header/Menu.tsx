import { Menu } from 'antd';
import styled from 'styled-components';

export const StyledMenu = styled(Menu)`
  && {
    background: transparent;
    border-bottom: 0;
  }
`;

export const StyledMenuItem = styled(Menu.Item)`
  &&& {
    font-size: 15px;
    font-weight: 500;
    line-height: 48px;
    height: 49px;
    & > a {
      &,
      &:hover {
        opacity: 0.6;
        color: white;
      }
    }
    &.ant-menu-item-selected,
    &.ant-menu-item-active {
      color: white;
      border-bottom: 2px solid white;

      &:hover {
        border-bottom: 2px solid white;
      }

      a {
        opacity: 1;
        color: white;
      }
    }

    &:not(.ant-menu-item-selected) {
      &.ant-menu-item-active,
      &:hover {
        color: white;
        border-bottom: 2px solid white;
      }
    }
  }
`;

type TStyledMenuButtonProps = {
  noMargin?: boolean;
};

export const StyledMenuButton = styled.a<TStyledMenuButtonProps>`
  &&& {
    float: right;
    height: 32px;
    margin: 8px ${(props) => (props.noMargin ? '0' : '8px')};
    border: 1px solid rgba(255, 255, 255, 0.35);
    border-radius: 4px;
    display: flex;
    align-items: center;
    color: white;
    transition: all 0.25s ease;
    padding: 0 10px;

    span {
      font-size: 14px;
      margin-right: 10px;
    }

    &:hover {
      border-color: white;
      color: white;
    }
  }
`;
