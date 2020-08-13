import React from 'react';
import { Tabs } from 'antd';
import Axes from '../Pivot/Axes';
import Options from '../Pivot/Options';
import OrderGroup from '../Order/OrderGroup';
import Limit from './Limit';

const { TabPane } = Tabs;

export default function Settings({
  pivotConfig,
  orderMembers,
  limit,
  onMove,
  onUpdate,
  onReorder,
  onOrderChange,
}) {
  return (
    <Tabs style={{ width: 340 }}>
      <TabPane tab="Pivot" key="1">
        <Axes pivotConfig={pivotConfig} onMove={onMove} />
      </TabPane>

      <TabPane tab="Pivot Options" key="2">
        <Options pivotConfig={pivotConfig} onUpdate={onUpdate} />
      </TabPane>

      <TabPane tab="Order" key="3">
        <OrderGroup
          orderMembers={orderMembers}
          onReorder={onReorder}
          onOrderChange={onOrderChange}
        />
      </TabPane>

      <TabPane tab="Limit" key="4">
        <Limit limit={limit} onUpdate={onUpdate} />
      </TabPane>
    </Tabs>
  );
}
