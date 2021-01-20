import * as PropTypes from 'prop-types';
import { Menu } from 'antd';
import {
  LineChartOutlined,
  AreaChartOutlined,
  BarChartOutlined,
  PieChartOutlined,
  TableOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons';
import ButtonDropdown from './ButtonDropdown';

const ChartTypes = [
  { name: 'line', title: 'Line', icon: <LineChartOutlined /> },
  { name: 'area', title: 'Area', icon: <AreaChartOutlined /> },
  { name: 'bar', title: 'Bar', icon: <BarChartOutlined /> },
  { name: 'pie', title: 'Pie', icon: <PieChartOutlined /> },
  { name: 'table', title: 'Table', icon: <TableOutlined /> },
  { name: 'number', title: 'Number', icon: <InfoCircleOutlined /> },
];

const SelectChartType = ({ chartType, updateChartType }) => {
  const menu = (
    <Menu>
      {ChartTypes.map((m) => (
        <Menu.Item key={m.title} onClick={() => updateChartType(m.name)}>
          {m.icon} {m.title}
        </Menu.Item>
      ))}
    </Menu>
  );

  const foundChartType = ChartTypes.find((t) => t.name === chartType);
  return (
    <ButtonDropdown overlay={menu} icon={foundChartType.icon} style={{ border: 0 }}>
      {foundChartType.title}
    </ButtonDropdown>
  );
};

SelectChartType.propTypes = {
  chartType: PropTypes.string.isRequired,
  updateChartType: PropTypes.func.isRequired,
};

export default SelectChartType;
