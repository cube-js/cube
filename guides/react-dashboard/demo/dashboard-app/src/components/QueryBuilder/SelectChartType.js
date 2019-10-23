import React from 'react';
import * as PropTypes from 'prop-types';
import {
  Menu, Icon, Dropdown
} from 'antd';

import styled from 'styled-components';

const StyledDropdownTrigger = styled.span`
  color: #43436B;
  cursor: pointer;
  margin-left: 13px;

  & > span {
    margin: 0 8px;
  }
`

const ChartTypes = [
  { name: 'line', title: 'Line', icon: 'line-chart' },
  { name: 'area', title: 'Area', icon: 'area-chart' },
  { name: 'bar', title: 'Bar', icon: 'bar-chart' },
  { name: 'pie', title: 'Pie', icon: 'pie-chart' },
  { name: 'table', title: 'Table', icon: 'table' },
  { name: 'number', title: 'Number', icon: 'info-circle' }
];

const SelectChartType = ({ chartType, updateChartType }) => {
  const menu = (
    <Menu>
      {ChartTypes.map(m => (
        <Menu.Item key={m.title} onClick={() => updateChartType(m.name)}>
          <Icon type={m.icon} />
          {m.title}
        </Menu.Item>
      ))}
    </Menu>
  );

  const foundChartType = ChartTypes.find(t => t.name === chartType);
  return (
    <Dropdown overlay={menu} icon={foundChartType.icon} lacement="bottomLeft" trigger={['click']}>
    <StyledDropdownTrigger>
      <Icon type={foundChartType.icon} />
      <span>{foundChartType.title}</span>
      <Icon type="caret-down" />
    </StyledDropdownTrigger>
    </Dropdown>
  );
};

SelectChartType.propTypes = {
  chartType: PropTypes.string.isRequired,
  updateChartType: PropTypes.func.isRequired
};

export default SelectChartType;
