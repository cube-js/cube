import React from 'react';
import { Tabs } from 'antd';
import Axes from './Axes';
import Options from './Options';
export default function Pivot({ pivotConfig, onMove, onUpdate }) {
  return (
    <Tabs
      style={{
        width: 340,
      }}
    >
      <Tabs.TabPane tab="Pivot" key="1">
        <Axes pivotConfig={pivotConfig} onMove={onMove} />
      </Tabs.TabPane>

      <Tabs.TabPane tab="Options" key="2">
        <Options pivotConfig={pivotConfig} onUpdate={onUpdate} />
      </Tabs.TabPane>
    </Tabs>
  );
}
