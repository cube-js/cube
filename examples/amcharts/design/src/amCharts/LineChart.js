import React, { Component } from 'react';
import PropTypes from 'prop-types';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';

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
    yAxis.fontSize = '11px';
    yAxis.fontWeight = '400';
    yAxis.renderer.labels.template.fill = am4core.color('#616061');

    const dateAxis = chart.xAxes.push(new am4charts.DateAxis());
    dateAxis.fontSize = '11px';
    dateAxis.fontWeight = '400';
    dateAxis.renderer.labels.template.fill = am4core.color('#616061');
    dateAxis.gridIntervals.setAll([{ timeUnit: 'month', count: 1 }]);
    dateAxis.dateFormats.setKey('month', 'MMM\nYYYY');
    dateAxis.periodChangeDateFormats.setKey('month', 'MMM\nYYYY');
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
      series.minBulletDistance = 15;

      const bullet = series.bullets.push(new am4charts.CircleBullet());
      bullet.circle.strokeWidth = 2;
      bullet.circle.radius = 3;
      bullet.circle.stroke = am4core.color(option.color);
      bullet.circle.fill = am4core.color('#fff');
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
