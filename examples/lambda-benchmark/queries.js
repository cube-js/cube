function getRandomInRange(min, max) {
  return min + Math.round(Math.random() * (max - min))
}

function pad(n, width, z) {
  z = z || '0';
  n = n + '';
  return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

export const queries = {
  GithubCommits: {
    data: () => {
      return {
        year1: 2019,
        month1: pad(getRandomInRange(1, 12), 2),
        day1: pad(getRandomInRange(1, 28), 2),
      }
    },

    query: ({year1, month1, day1}) => ({
      "order": {
        "GithubCommits.count": "desc"
      },
      "measures": [
        "GithubCommits.count",
      ],
      "dimensions": [
        "GithubCommits.repo"
      ],
      "timeDimensions": [
        {
          "dimension": "GithubCommits.time",
          "granularity": "day",
          "dateRange": [
            `${year1}-${month1}-${day1}`,
            `2022-01-01`
          ]
        }
      ]
    }),
  },

  GdeltEvents: {
    data: () => {
      return {
        year1: 2019,
        month1: pad(getRandomInRange(1, 12), 2),
        day1: pad(getRandomInRange(1, 28), 2),
      }
    },

    query: ({year1, month1, day1}) => ({
      "order": {
        "GdeltEvents.count": "desc"
      },
      "measures": [
        "GdeltEvents.count",
      ],
      "dimensions": [
        "GdeltEvents.code"
      ],
      "timeDimensions": [
        {
          "dimension": "GdeltEvents.time",
          "granularity": "day",
          "dateRange": [
            `${year1}-${month1}-${day1}`,
            `2022-01-01`
          ]
        }
      ]
    }),
  },

  Mobility: {
    data: () => {
      return {
        year1: 2021,
        month1: pad(getRandomInRange(1, 12), 2),
        day1: pad(getRandomInRange(1, 28), 2),
      }
    },

    query: ({year1, month1, day1}) => ({
      "order": {
        "Mobility.count": "desc"
      },
      "measures": [
        "Mobility.count",
      ],
      "dimensions": [
        "Mobility.country"
      ],
      "timeDimensions": [
        {
          "dimension": "Mobility.time",
          "granularity": "day",
          "dateRange": [
            `${year1}-${month1}-${day1}`,
            `2022-01-01`
          ]
        }
      ]
    }),
  },
};
