cube(`Stocks`, {
  sql: `SELECT * FROM default.stocks`,

  measures: {
    count: { sql: `${CUBE}.Date`, type: `count` },
    open: { sql: `${CUBE}.Open`, type: `avg`, format: `currency` },
    close: { sql: `${CUBE}.Close`, type: `avg`, format: `currency` },
    high: { sql: `${CUBE}.High`, type: `max`, format: `currency` },
    low: { sql: `${CUBE}.Low`, type: `min`, format: `currency` },
    volume: { sql: `${CUBE}.Volume`, type: `sum`, format: `currency` },
    firstTraded: { sql: `${CUBE}.Date`, type: `min` },
  },

  dimensions: {
    ticker: { sql: `${CUBE}.Ticker`, type: `string` },
    date: { sql: `${CUBE}.Date`, type: `time` },
  },
});
