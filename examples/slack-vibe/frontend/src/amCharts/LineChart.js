import React, { Component } from 'react';
import PropTypes from 'prop-types';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';
import { styleDateAxisFormats, styleAxis } from './styles';

const intervals = [
  { timeUnit: 'hour', count: 1 },
  { timeUnit: 'day', count: 1 },
  { timeUnit: 'week', count: 1 },
  { timeUnit: 'month', count: 1 },
];

class LineChart extends Component {
  constructor(props) {
    super(props);

    this.state = { id: 'line' + Math.random() };
  }

  componentDidMount() {
    const { options } = this.props;

    const chart = am4core.create(this.state.id, am4charts.XYChart);

    chart.maskBullets = false;
    const yAxis = chart.yAxes.push(new am4charts.ValueAxis());
    styleAxis(yAxis);

    const dateAxis = chart.xAxes.push(new am4charts.DateAxis());
    styleAxis(dateAxis);
    styleDateAxisFormats(dateAxis);
    dateAxis.gridIntervals.setAll(intervals);
    dateAxis.renderer.grid.template.location = 0.5;
    dateAxis.renderer.labels.template.location = 0.5;
    dateAxis.startLocation = 0.5;
    dateAxis.endLocation = 0.5;

    for (const option of options) {
      const series = chart.series.push(new am4charts.LineSeries());
      series.stroke = am4core.color(option.color);
      series.dataFields.valueY = option.y;
      series.dataFields.dateX = option.x;
      series.strokeWidth = 2;
    }

    this.chart = chart;
  }

  componentDidUpdate(oldProps) {
    if (oldProps.data !== this.props.data) {
      this.chart.data = this.props.data;
    }
  }

  componentWillUnmount() {
    if (this.chart) {
      this.chart.dispose();
    }
  }

  render() {
    return (
      <div id={this.state.id} style={{ width: '100%', height: '300px' }} />
    );
  }
}

export default LineChart;

LineChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  options: PropTypes.arrayOf(PropTypes.object).isRequired,
};
