function getRandomInRange(min, max) {
  return min + Math.round(Math.random() * (max - min))
}

function pad(n, width, z) {
  z = z || '0';
  n = n + '';
  return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

export default [
  {
    name: 'order-count-by-created-at-by-day',

    data: () => {
      const year = getRandomInRange(2019, 2021)

      return {
        year1: year,
        year2: year + 1,
        month1: pad(getRandomInRange(1, 12), 2),
        month2: pad(getRandomInRange(1, 12), 2),
        day1: pad(getRandomInRange(1, 28), 2),
        day2: pad(getRandomInRange(1, 28), 2),
      }
    },

    query: ({ year1, year2, month1, month2, day1, day2 }) => ({
      "order": {
        "Mobility.grocery": "desc"
      },
      "measures": [
        "Mobility.grocery",
        "Mobility.park",
        "Mobility.residential",
      ],
      "dimensions": [
        "Mobility.country"
      ],
      "timeDimensions": [
        {
          "dimension": "Mobility.date",
          "granularity": "day",
          "dateRange": [
            `${year1}-${month1}-${day1}`,
            `${year2}-${month2}-${day2}`,
          ]
        }
      ]
    }),
  },
];