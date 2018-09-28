'use strict';

const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');
cubejsApi.load({
  measures: ["Stories.count"],
  timeDimensions: [{
    dimension: "Stories.time",
    dateRange: ["2015-01-01", "2016-01-01"],
    granularity: 'month'
  }]
})
  .then(r => {
    const context = document.getElementById("myChart");
    new Chart(context, chartjsConfig(r));
  });