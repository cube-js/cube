import React from 'react';
import cx from 'classnames';
import { Menu } from 'antd';
import Link from './Link';

import { trimSlashes } from '../Layout';

type Props = {
  selectedKeys?: string[];
  to: string;
  title: string;
};

const MenuItem: React.FC<Props> = (props) => (
  <Menu.Item
    {...props}
    title={null}
    className={cx({
      'ant-menu-item-selected': props.selectedKeys?.includes(
        trimSlashes(props.to)
      ),
    })}
    key={trimSlashes(props.to)}
  >
    <Link to={props.to}>{props.title}</Link>
  </Menu.Item>
);

export default MenuItem;
