import moment from 'moment';
import numeral from 'numeral';

export const formatters = {
  date: (val) => moment(val).format("MMM DD"),
  number: (val) => numeral(val).format('0,0'),
  undefined: (val) => val
}

export const format = (key, data, formatter) => (
  data.map(i => {
    i[key] = formatters[formatter](i[key]);
    return i;
  })
);

// 99% per https://github.com/recharts/recharts/issues/172
export const RECHARTS_RESPONSIVE_WIDTH = "99%";

export const PRIMARY_COLOR = "#7DB3FF";

export const COLORS = [
  PRIMARY_COLOR,
  "#49457B",
  "#FF7C78",
  "#FED3D0",
  "#6F76D9",
  "#9ADFB4",
  "#2E7987"
];

export const DASHBOARD_CHART_MIN_HEIGHT = 320;
