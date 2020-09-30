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
    var interfaceColors = new am4core.InterfaceColorSet();
    // Set map definition
    chart.geodata = am4geodata_worldTimeZoneAreasLow;
    chart.responsive.enabled = true;
    chart.chartContainer.wheelable = false;
    // Set projection
    chart.projection = new am4maps.projections.Miller();
    chart.panBehavior = 'move';

    // Create map polygon series
    var polygonSeries = chart.series.push(new am4maps.MapPolygonSeries());

    //Set min/max fill color for each area
    polygonSeries.heatRules.push({
      property: 'fill',
      target: polygonSeries.mapPolygons.template,
      min: chart.colors.getIndex(1).brighten(1),
      max: chart.colors.getIndex(1).brighten(-0.3),
    });

    // Make map load polygon data (state shapes and names) from GeoJSON
    polygonSeries.useGeodata = true;

    // Configure series tooltip
    var polygonTemplate = polygonSeries.mapPolygons.template;
    polygonTemplate.tooltipText = '{id}: {value}';

    // Create map polygon series
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
    var boundTemplate = boundsSeries.mapPolygons.template;

    // Create hover state and set alternative fill color
    var hs = boundTemplate.states.create('hover');
    hs.properties.fill = am4core.color('#3c5bdc');
    //hs.properties.fillOpacity = 0.2;

    boundsSeries.mapPolygons.template.events.on('over', function (event) {
      var polygon = polygonSeries.getPolygonById(
        event.target.dataItem.dataContext.id
      );
      if (polygon) {
        polygon.isHover = true;
      }
    });

    boundsSeries.mapPolygons.template.events.on('out', function (event) {
      var polygon = polygonSeries.getPolygonById(
        event.target.dataItem.dataContext.id
      );
      if (polygon) {
        polygon.isHover = false;
      }
    });

    var labelSeries = chart.series.push(new am4maps.MapImageSeries());
    var label = labelSeries.mapImages.template.createChild(am4core.Label);
    label.text = '{id}';
    label.strokeOpacity = 1;
    label.fill = am4core.color('#000000');
    label.horizontalCenter = 'middle';
    label.fontSize = 8;
    label.nonScaling = false;

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

    this.polygonSeries = polygonSeries;
  }

  componentDidUpdate(oldProps) {
    if (oldProps.data !== this.props.data) {
      this.polygonSeries.data = this.props.data;
    }
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
