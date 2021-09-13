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
        month1: pad(getRandomInRange(4, 7), 2),
        month2: pad(getRandomInRange(4, 7), 2),
        day1: pad(getRandomInRange(1, 28), 2),
        day2: pad(getRandomInRange(1, 28), 2),
      }
    },
    query: ({ month1, month2, day1, day2 }) => {
      return {
        "measures": [
          "Events.count"
        ],
        "timeDimensions": [
          {
            "dimension": "Events.createdAt",
            "granularity": "day",
            "dateRange": [
              `2012-${month1}-${day1}`,
              `2012-${month2}-${day2}`,
            ]
          }
        ],
        "order": {
          "Events.count": "desc"
        },
        "dimensions": [
          "Events.repositoryName"
        ],
        "filters": [
          {
            "member": "Events.type",
            "operator": "equals",
            "values": [
              "WatchEvent"
            ]
          }
        ],
        "limit": 1000
      };
    },
  }
};
