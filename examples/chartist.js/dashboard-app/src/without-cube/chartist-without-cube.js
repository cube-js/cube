import 'chartist/dist/index.css';
import { LineChart } from 'chartist';

new LineChart(
  '#chartist__example',
  {
    labels: [1, 2, 3, 4, 5, 6, 7, 8],
    series: [[5, 9, 7, 8, 5, 3, 5, 4]]
  },
  {
    low: 0,
    showArea: true
  }
);
