function getRandomInRange(min, max) {
	const rand = min + Math.round(Math.random() * (max - min));
  return rand < 10 ? '0'.concat(rand) : String(rand);
}

function pad(n, width, z) {
  z = z || '0';
  n = n + '';
  return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

export default {
  generate: {
    data: () => {
      return {
        year1: pad(getRandomInRange(1990, 1999), 2),
        year2: 2000,
        month1: pad(getRandomInRange(1, 12), 2),
        month2: pad(getRandomInRange(1, 12), 2),
        day1: pad(getRandomInRange(1, 28), 2),
        day2: pad(getRandomInRange(1, 28), 2),
      }
    },
    query: ({ year1, year2, month1, month2, day1, day2 }) => {
      return {
        "measures": [
          "Orders.count"
        ],
        "timeDimensions": [
          {
            "dimension": "Orders.oOrderdate",
            "granularity": "day",
            "dateRange": [
              `${year1}-${month1}-${day1}`,
              `${year2}-${month2}-${day2}`
            ]
          }
        ],
        "order": {
          "Orders.count": "desc"
        },
        "dimensions": [
          "Orders.oOrderstatus"
        ],
        "limit": 10000
      };
    },
  }
};
