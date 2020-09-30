import React, { Component } from 'react';
import PropTypes from 'prop-types';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';
import { styleAxis } from './styles';

class Heatmap extends Component {
  constructor(props) {
    super(props);

    this.state = { id: 'heatmap' + Math.random() };
  }

  componentDidMount() {
    const { options } = this.props;

    const chart = am4core.create(this.state.id, am4charts.XYChart);
    chart.maskBullets = false;

    const xAxis = chart.xAxes.push(new am4charts.CategoryAxis());
    styleAxis(xAxis);
    xAxis.dataFields.category = 'month';
    xAxis.renderer.grid.template.disabled = true;
    xAxis.renderer.minGridDistance = 12;

    const yAxis = chart.yAxes.push(new am4charts.CategoryAxis());
    styleAxis(yAxis);
    yAxis.dataFields.category = 'weekday';
    yAxis.renderer.grid.template.disabled = true;
    yAxis.renderer.inversed = true;

    const series = chart.series.push(new am4charts.ColumnSeries());
    series.dataFields.categoryX = 'month';
    series.dataFields.categoryY = 'weekday';
    series.dataFields.value = 'value';
    series.sequencedInterpolation = true;

    const columnTemplate = series.columns.template;
    columnTemplate.strokeWidth = 3;
    columnTemplate.strokeOpacity = 1;
    columnTemplate.stroke = new am4core.color('#fff');
    columnTemplate.width = am4core.percent(100);
    columnTemplate.height = am4core.percent(100);

    series.heatRules.push({
      target: columnTemplate,
      property: 'fill',
      min: new am4core.color(options.colors.min),
      max: new am4core.color(options.colors.max),
    });

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

export default Heatmap;

Heatmap.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  options: PropTypes.object.isRequired,
};
