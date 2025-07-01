import { useState } from 'react';
import { Menu } from 'antd';
import {
  LineChartOutlined,
  AreaChartOutlined,
  BarChartOutlined,
  PieChartOutlined,
  TableOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons';
import { ButtonDropdown } from './ButtonDropdown';

const ChartTypes = [
  { name: 'line', title: 'Line', icon: <LineChartOutlined /> },
  { name: 'area', title: 'Area', icon: <AreaChartOutlined /> },
  { name: 'bar', title: 'Bar', icon: <BarChartOutlined /> },
  { name: 'pie', title: 'Pie', icon: <PieChartOutlined /> },
  { name: 'table', title: 'Table', icon: <TableOutlined /> },
  { name: 'number', title: 'Number', icon: <InfoCircleOutlined /> },
];

const SelectChartType = ({ chartType, updateChartType }) => {
  const [shown, setShown] = useState(false);

  const menu = (
    <div className="test simple-overlay">
      <Menu data-testid="chart-type-dropdown" className="ant-dropdown-menu ant-dropdown-menu-root">
        {ChartTypes.map((m) => (
          <Menu.Item key={m.title} className="ant-dropdown-menu-item" onClick={() => updateChartType(m.name)}>
            {m.icon} {m.title}
          </Menu.Item>
        ))}
      </Menu>
    </div>
  );

  const foundChartType = ChartTypes.find((t) => t.name === chartType);
  return (
    <ButtonDropdown
      show={shown}
      data-testid="chart-type-btn"
      overlay={menu}
      icon={foundChartType?.icon}
      style={{ border: 0 }}
      onOverlayOpen={() => setShown(true)}
      onOverlayClose={() => setShown(false)}
      onItemClick={() => setShown(false)}
    >
      {foundChartType?.title || ''}
    </ButtonDropdown>
  );
};

export default SelectChartType;
