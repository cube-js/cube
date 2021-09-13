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
        month1: pad(getRandomInRange(1, 12), 2),
        month2: pad(getRandomInRange(1, 12), 2),
        day1: pad(getRandomInRange(1, 28), 2),
        day2: pad(getRandomInRange(1, 28), 2),
        hour1: pad(getRandomInRange(0, 23), 2),
        hour2: pad(getRandomInRange(0, 23), 2),
      }
    },
    query: ({ month1, month2, day1, day2, hour1, hour2 }) => {
      return `
        SELECT
          repository_name,
          type,
          DATE_TRUNC(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at), DAY) as day,
          count(*)
        FROM \`cube-devrel-team.github.events\`
        WHERE 
            type = "WatchEvent" AND
            PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at) > PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2012-${month1}-${day1} ${hour1}:00:00') AND
            PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at) < PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2012-${month2}-${day2} ${hour2}:00:00')
        GROUP BY 1, 2, 3
        ORDER BY 4 DESC
        LIMIT 1000;  
      `;
    },
  }
};
