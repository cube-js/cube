import React, { Component } from 'react';
import * as am4core from '@amcharts/amcharts4/core';
import * as am4maps from '@amcharts/amcharts4/maps';
import am4geodata_worldTimeZoneAreasLow from '@amcharts/amcharts4-geodata/worldTimeZoneAreasLow';
import am4geodata_worldTimeZonesLow from '@amcharts/amcharts4-geodata/worldTimeZonesLow';
//import am4geodata_worldLow from '@amcharts/amcharts4-geodata/worldLow';
import am4themes_animated from '@amcharts/amcharts4/themes/animated';

am4core.useTheme(am4themes_animated);

class Map extends Component {
  constructor(props) {
    super(props);
    this.state = { id: 'map' + Math.random() };
  }
  componentDidMount() {
    let chart = am4core.create(this.state.id, am4maps.MapChart);
    // Set map definition
    chart.geodata = am4geodata_worldTimeZoneAreasLow;
    chart.responsive.enabled = true;
    chart.chartContainer.wheelable = false;
    // Set projection
    chart.projection = new am4maps.projections.Miller();
    chart.panBehavior = 'move';

    var interfaceColors = new am4core.InterfaceColorSet();

    // Create map polygon series
    var polygonSeries = chart.series.push(new am4maps.MapPolygonSeries());
    // Make map load polygon (like country names) data from GeoJSON
    polygonSeries.useGeodata = true;
    polygonSeries.calculateVisualCenter = true;

    // Configure series
    var polygonTemplate = polygonSeries.mapPolygons.template;
    polygonTemplate.propertyFields.fill = 'fill';
    polygonTemplate.adapter.add('fill', function (fill, target) {
      console.log(target.dataItem.dataContext);
      if (target.dataItem.index > 0) {
        return chart.colors.getIndex(target.dataItem.index);
      }
      return fill;
    });

    var boundsSeries = chart.series.push(new am4maps.MapPolygonSeries());
    boundsSeries.geodata = am4geodata_worldTimeZonesLow;
    boundsSeries.useGeodata = true;
    boundsSeries.mapPolygons.template.fill = am4core.color(
      interfaceColors.getFor('alternativeBackground')
    );
    boundsSeries.mapPolygons.template.fillOpacity = 0.07;
    boundsSeries.mapPolygons.template.nonScalingStroke = true;
    boundsSeries.mapPolygons.template.strokeWidth = 0.5;
    boundsSeries.mapPolygons.template.strokeOpacity = 1;
    boundsSeries.mapPolygons.template.stroke = interfaceColors.getFor(
      'background'
    );
    boundsSeries.tooltipText = '{id}';

    var hs = polygonTemplate.states.create('hover');
    hs.properties.fillOpacity = 1;

    var bhs = boundsSeries.mapPolygons.template.states.create('hover');
    bhs.properties.fillOpacity = 0;

    boundsSeries.mapPolygons.template.events.on('over', function (event) {
      var polygon = boundsSeries.getPolygonById(
        event.target.dataItem.dataContext.id
      );
      if (polygon) {
        polygon.isHover = true;
      }
    });

    boundsSeries.mapPolygons.template.events.on('out', function (event) {
      var polygon = boundsSeries.getPolygonById(
        event.target.dataItem.dataContext.id
      );
      if (polygon) {
        polygon.isHover = false;
      }
    });

    var labelSeries = chart.series.push(new am4maps.MapImageSeries());
    var label = labelSeries.mapImages.template.createChild(am4core.Label);
    label.text = '{id}';
    label.strokeOpacity = 0;
    label.fill = am4core.color('#000000');
    label.horizontalCenter = 'middle';
    label.fontSize = 9;
    label.nonScaling = true;

    labelSeries.mapImages.template.adapter.add(
      'longitude',
      (longitude, target) => {
        target.zIndex = 100000;

        var polygon = polygonSeries.getPolygonById(
          target.dataItem.dataContext.id
        );
        if (polygon) {
          return polygon.visualLongitude;
        }
        return longitude;
      }
    );

    labelSeries.mapImages.template.adapter.add(
      'latitude',
      (latitude, target) => {
        var polygon = polygonSeries.getPolygonById(
          target.dataItem.dataContext.id
        );
        if (polygon) {
          return polygon.visualLatitude;
        }
        return latitude;
      }
    );
    polygonSeries.events.on('datavalidated', function () {
      labelSeries.data = polygonSeries.data;
    });

    this.chart = chart;
  }

  componentDidUpdate(oldProps) {
    /*if (oldProps.data !== this.props.data) {
      this.chart.data = this.props.data;
    }*/
  }

  componentWillUnmount() {
    if (this.chart) {
      this.chart.dispose();
    }
  }

  render() {
    return (
      <div id={this.state.id} style={{ width: '100%', height: '500px' }}></div>
    );
  }
}

export default Map;
