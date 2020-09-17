import React, { Component } from 'react';
import PropTypes from 'prop-types';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';
import { styleAxis } from './styles';

class Bubble extends Component {
  constructor(props) {
    super(props);

    this.state = { id: 'bubble' + Math.random() };
  }

  componentDidMount() {
    const { options } = this.props;
    let chart = am4core.create(this.state.id, am4charts.XYChart);

    chart.maskBullets = false;

    const xAxis = chart.xAxes.push(new am4charts.CategoryAxis());
    styleAxis(xAxis);
    xAxis.renderer.minGridDistance = 40;
    xAxis.dataFields.category = 'hour';
    xAxis.renderer.grid.template.disabled = true;
    xAxis.renderer.axisFills.template.disabled = true;
    xAxis.renderer.ticks.template.disabled = true;

    const yAxis = chart.yAxes.push(new am4charts.CategoryAxis());
    styleAxis(yAxis);
    yAxis.dataFields.category = 'weekday';
    yAxis.renderer.grid.template.disabled = true;
    yAxis.renderer.axisFills.template.disabled = true;
    yAxis.renderer.ticks.template.disabled = true;
    yAxis.renderer.inversed = true;

    const series = chart.series.push(new am4charts.ColumnSeries());
    series.dataFields.categoryY = 'weekday';
    series.dataFields.categoryX = 'hour';
    series.dataFields.value = 'value';
    series.columns.template.disabled = true;
    series.sequencedInterpolation = true;

    const bullet = series.bullets.push(new am4core.Circle());
    bullet.strokeWidth = 0;
    bullet.fill = am4core.color(options.color);

    bullet.adapter.add('tooltipY', function (tooltipY, target) {
      return -target.radius + 1;
    });

    series.heatRules.push({
      property: 'radius',
      target: bullet,
      min: 3,
      max: 13,
    });

    bullet.hiddenState.properties.scale = 0.01;
    bullet.hiddenState.properties.opacity = 1;

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
      <div id={this.state.id} style={{ width: '100%', height: '350px' }} />
    );
  }
}

export default Bubble;

Bubble.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  options: PropTypes.object.isRequired,
};
