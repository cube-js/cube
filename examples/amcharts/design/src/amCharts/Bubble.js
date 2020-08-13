import React, { Component } from 'react';
import PropTypes from 'prop-types';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';

class Bubble extends Component {
  constructor(props) {
    super(props);

    this.state = { id: 'bubble' + Math.random() };
  }

  componentDidMount() {
    const { options } = this.props;
    let chart = am4core.create(this.state.id, am4charts.XYChart);

    chart.maskBullets = false;

    var xAxis = chart.xAxes.push(new am4charts.CategoryAxis());
    var yAxis = chart.yAxes.push(new am4charts.CategoryAxis());

    yAxis.dataFields.category = 'weekday';
    xAxis.renderer.minGridDistance = 40;
    xAxis.dataFields.category = 'hour';

    xAxis.renderer.grid.template.disabled = true;
    yAxis.renderer.grid.template.disabled = true;
    xAxis.renderer.axisFills.template.disabled = true;
    yAxis.renderer.axisFills.template.disabled = true;
    yAxis.renderer.ticks.template.disabled = true;
    xAxis.renderer.ticks.template.disabled = true;

    yAxis.renderer.inversed = true;

    var series = chart.series.push(new am4charts.ColumnSeries());
    series.dataFields.categoryY = 'weekday';
    series.dataFields.categoryX = 'hour';
    series.dataFields.value = 'value';
    series.columns.template.disabled = true;
    series.sequencedInterpolation = true;
    //series.defaultState.transitionDuration = 3000;

    var bullet = series.bullets.push(new am4core.Circle());
    bullet.tooltipText =
      "{weekday}, {hour}: {value.workingValue.formatNumber('#.')}";
    bullet.strokeWidth = 3;
    bullet.stroke = am4core.color('#ffffff');
    bullet.strokeOpacity = 0;
    bullet.fillOpacity = 0.7;

    bullet.adapter.add('tooltipY', function (tooltipY, target) {
      return -target.radius + 1;
    });

    series.heatRules.push({
      property: 'radius',
      target: bullet,
      min: 2,
      max: 30,
    });

    bullet.hiddenState.properties.scale = 0.01;
    bullet.hiddenState.properties.opacity = 1;

    var hoverState = bullet.states.create('hover');
    hoverState.properties.strokeOpacity = 1;

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
      <div id={this.state.id} style={{ width: '100%', height: '400px' }} />
    );
  }
}

export default Bubble;

Bubble.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  options: PropTypes.arrayOf(PropTypes.object).isRequired,
};
