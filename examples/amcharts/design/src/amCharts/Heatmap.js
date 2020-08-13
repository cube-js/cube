import React, { Component } from 'react';
import PropTypes from 'prop-types';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';

class Heatmap extends Component {
  constructor(props) {
    super(props);

    this.state = { id: 'heatmap' + Math.random() };
  }

  componentDidMount() {
    const { options } = this.props;

    const chart = am4core.create(this.state.id, am4charts.XYChart);
    chart.maskBullets = false;

    var xAxis = chart.xAxes.push(new am4charts.CategoryAxis());
    var yAxis = chart.yAxes.push(new am4charts.CategoryAxis());

    xAxis.dataFields.category = 'month';
    yAxis.dataFields.category = 'weekday';

    xAxis.renderer.grid.template.disabled = true;
    xAxis.renderer.minGridDistance = 40;

    yAxis.renderer.grid.template.disabled = true;
    yAxis.renderer.inversed = true;
    yAxis.renderer.minGridDistance = 40;

    var series = chart.series.push(new am4charts.ColumnSeries());
    series.dataFields.categoryX = 'month';
    series.dataFields.categoryY = 'weekday';
    series.dataFields.value = 'value';
    series.sequencedInterpolation = true;
    series.defaultState.transitionDuration = 3000;

    var bgColor = new am4core.InterfaceColorSet().getFor('background');

    var columnTemplate = series.columns.template;
    columnTemplate.strokeWidth = 1;
    columnTemplate.strokeOpacity = 0.2;
    columnTemplate.stroke = bgColor;
    columnTemplate.tooltipText =
      "{weekday}, {month}: {value.workingValue.formatNumber('#.')}";
    columnTemplate.width = am4core.percent(100);
    columnTemplate.height = am4core.percent(100);

    series.heatRules.push({
      target: columnTemplate,
      property: 'fill',
      min: am4core.color(bgColor),
      max: chart.colors.getIndex(0),
    });

    // heat legend
    var heatLegend = chart.bottomAxesContainer.createChild(
      am4charts.HeatLegend
    );
    heatLegend.width = am4core.percent(100);
    heatLegend.series = series;
    heatLegend.valueAxis.renderer.labels.template.fontSize = 9;
    heatLegend.valueAxis.renderer.minGridDistance = 40;

    // heat legend behavior
    series.columns.template.events.on('over', function (event) {
      handleHover(event.target);
    });

    series.columns.template.events.on('hit', function (event) {
      handleHover(event.target);
    });

    function handleHover(column) {
      if (!isNaN(column.dataItem.value)) {
        heatLegend.valueAxis.showTooltipAt(column.dataItem.value);
      } else {
        heatLegend.valueAxis.hideTooltip();
      }
    }

    series.columns.template.events.on('out', function (event) {
      heatLegend.valueAxis.hideTooltip();
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
      <div id={this.state.id} style={{ width: '100%', height: '400px' }} />
    );
  }
}

export default Heatmap;

Heatmap.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  options: PropTypes.arrayOf(PropTypes.object).isRequired,
};
