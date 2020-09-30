import * as am4core from "@amcharts/amcharts4/core"

export function styleAxis(axis) {
  axis.fontSize = '11px';
  axis.fontWeight = '400';
  axis.renderer.labels.template.fill = am4core.color('#727290');
}

const formats = {
  'week': "'W' ww\nYYYY",
  'month': 'MMM\nYYYY',
};

export function styleDateAxisFormats(axis) {
  Object.keys(formats).forEach(format => {
    axis.dateFormats.setKey(format, formats[format]);
    axis.periodChangeDateFormats.setKey(format, formats[format]);
  });
}