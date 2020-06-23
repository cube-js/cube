import React from 'react';
import { Tabs } from 'antd';
import Axes from './Axes';
import Options from './Options';

const { TabPane } = Tabs;

export default function Pivot({ pivotConfig, onMove, onUpdate }) {
  return (
    <Tabs style={{ width: 340 }}>
      <TabPane tab="Pivot" key="1">
        <Axes pivotConfig={pivotConfig} onMove={onMove} />
      </TabPane>

      <TabPane tab="Options" key="2">
        <Options pivotConfig={pivotConfig} onUpdate={onUpdate} />
      </TabPane>
    </Tabs>
  );
}
