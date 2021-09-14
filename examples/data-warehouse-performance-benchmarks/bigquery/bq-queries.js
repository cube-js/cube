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
      return `
        SELECT
          O_ORDERSTATUS AS Order_Status,
          DATE_TRUNC(
              O_ORDERDATE,
              DAY
          ) AS Order_Day,
          COUNT(O_ORDERKEY) AS Order_Key
        FROM
          \`cubejs-k8s-cluster.cubejs_benchmarks_tpch_sf100.orders\`
        WHERE
          (
            O_ORDERDATE >= '${year1}-${month1}-${day1}'
            AND O_ORDERDATE <= '${year2}-${month2}-${day2}'
          )
        GROUP BY
          1,
          2
        ORDER BY
          3 DESC
        LIMIT
          10000;
      `;
    },
  }
};
