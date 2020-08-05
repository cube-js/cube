import React from 'react';
import { Table, Tag, Space } from 'antd';

export default (props) => {
  return <Table columns={props.data.column} dataSource={props.data.data} />;
};
