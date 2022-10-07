import { Chart } from '@trigensoftware/chart.js/auto'

const DATA_COUNT = 7;
const NUMBER_CFG = {count: DATA_COUNT, min: -100, max: 100};

const labels = ['Jan','Feb','Mar','Apr','May','Jun','Jul'];
const data = {
  labels: labels,
  datasets: [
    {
      label: 'Dataset 1',
      data: [1,2,3,4,5,6,7].map(i => Math.ceil(i + Math.random() * 3)),
    },
    {
      label: 'Dataset 2',
      data: [1,2,3,4,5,6,7].map(i => Math.ceil(i + Math.random() * 3)),
    },
    {
      label: 'Dataset 3',
      data: [1,2,3,4,5,6,7].map(i => Math.ceil(i + Math.random() * 3)),
    },
    {
      label: 'Dataset 4',
      data: [1,2,3,4,5,6,7].map(i => Math.ceil(i + Math.random() * 3)),
    },
    {
      label: 'Dataset 5',
      data: [1,2,3,4,5,6,7].map(i => Math.ceil(i + Math.random() * 3)),
    },
    {
      label: 'Dataset 6',
      data: [1,2,3,4,5,6,7].map(i => Math.ceil(i + Math.random() * 3)),
    },
    {
      label: 'Dataset 7',
      data: [1,2,3,4,5,6,7].map(i => Math.ceil(i + Math.random() * 3)),
    },
  ]
};

const config = {
  type: 'line',
  data: data,
  options: {
    indexAxis: 'y',
  //   // Elements options apply to all of the options unless overridden in a dataset
  //   // In this case, we are setting the border of each horizontal bar to be 2px wide
    // elements: {
    //   bar: {
    //     borderWidth: 2,
    //   }
    // },
  //   plugins: {
  //     legend: {
  //       position: 'right',
  //     },
  //     title: {
  //       display: true,
  //       text: 'Chart.js Horizontal Bar Chart'
  //     }
  //   }
  },
};

new Chart(
  document.getElementById('artists'),
  config
);