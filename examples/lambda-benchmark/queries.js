function getRandomInRange(min, max) {
  return min + Math.round(Math.random() * (max - min))
}

function pad(n, width, z) {
  z = z || '0';
  n = n + '';
  return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

export const queries = {
  githubCommits: {
    name: 'commits-count-by-day-and-repo',

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
          "dimension": "GithubCommits.date",
          "granularity": "day",
          // "granularity": "week",
          "dateRange": [
            `${year1}-${month1}-${day1}`,
            `2022-01-01`
          ]
        }
      ]
    }),
  }
};
