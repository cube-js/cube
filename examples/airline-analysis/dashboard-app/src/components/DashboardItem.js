import { Card } from 'antd';
import React from 'react';

const DashboardItem = ({
  children,
  title
}) => <Card title={title} style={{
  height: '20rem',
  overflowY: 'auto',
  width: '100%',
  margin: '20px 0'
}}>
    {children}
  </Card>;

export default DashboardItem;