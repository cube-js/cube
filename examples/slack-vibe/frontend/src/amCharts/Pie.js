import React, { Component } from 'react';
import PropTypes from 'prop-types';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';

function hideSmall(ev) {
  if (ev.target.dataItem && ev.target.dataItem.values.value.percent < 4) {
    ev.target.hide();
  } else {
    ev.target.show();
  }
}

class Pie extends Component {
  constructor(props) {
    super(props);
    this.state = { id: 'pie' + Math.random() };
  }

  componentDidMount() {
    let chart = am4core.create(this.state.id, am4charts.PieChart);

    // Add and configure Series
    var pieSeries = chart.series.push(new am4charts.PieSeries());
    pieSeries.dataFields.value = 'value';
    pieSeries.dataFields.category = 'title';
    pieSeries.slices.template.stroke = am4core.color('#fff');
    pieSeries.slices.template.strokeOpacity = 1;
    pieSeries.labels.template.wrap = true;
    pieSeries.labels.template.fontSize = 13;
    pieSeries.alignLabels = false;
    pieSeries.ticks.template.disabled = true;

    pieSeries.tooltip.fontSize = 13;

    pieSeries.ticks.template.events.on('ready', hideSmall);
    pieSeries.ticks.template.events.on('visibilitychanged', hideSmall);
    pieSeries.labels.template.events.on('ready', hideSmall);
    pieSeries.labels.template.events.on('visibilitychanged', hideSmall);

    chart.hiddenState.properties.radius = am4core.percent(0);

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

export default Pie;

Pie.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  options: PropTypes.arrayOf(PropTypes.object).isRequired,
};
