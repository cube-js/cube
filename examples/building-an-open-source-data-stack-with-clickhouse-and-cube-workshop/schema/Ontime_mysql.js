cube(`Ontime_mysql`, {
  sql: `SELECT * FROM ontime`,
  dataSource: `mysql`,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [origincityname, originstatename, destcityname, deststatename, flightdate]
    },
    /**
     * Demo
     * Add custom measures
     */
    // avgDepDelay: {
    //   type: `avg`,
    //   sql: `${CUBE}.DepDelay`
    // },
    // avgDepDelayGreaterThanTenMinutesPercentage: {
    //   type: `number`,
    //   sql: `avg(${CUBE}.DepDelay>10)*100`,
    //   format: `percent`
    // },
  },
  
  dimensions: {
    year: {
      sql: `${CUBE}.Year`,
      type: `number`
    },
    quarter: {
      sql: `${CUBE}.Quarter`,
      type: `number`
    },
    month: {
      sql: `${CUBE}.Month`,
      type: `number`
    },
    dayofmonth: {
      sql: `${CUBE}.DayofMonth`,
      type: `number`
    },
    dayofweek: {
      sql: `${CUBE}.DayOfWeek`,
      type: `number`
    },
    airlineid: {
      sql: `${CUBE}.AirlineID`,
      type: `number`
    },
    uniquecarrier: {
      sql: `${CUBE}.UniqueCarrier`,
      type: `string`
    },
    carrier: {
      sql: `${CUBE}.Carrier`,
      type: `string`
    },
    tailnum: {
      sql: `${CUBE}.TailNum`,
      type: `string`
    },
    flightnum: {
      sql: `${CUBE}.FlightNum`,
      type: `string`
    },
    origin: {
      sql: `${CUBE}.Origin`,
      type: `string`
    },
    origincityname: {
      sql: `${CUBE}.OriginCityName`,
      type: `string`
    },
    originstate: {
      sql: `${CUBE}.OriginState`,
      type: `string`
    },
    originstatefips: {
      sql: `${CUBE}.OriginStateFips`,
      type: `string`
    },
    originstatename: {
      sql: `${CUBE}.OriginStateName`,
      type: `string`
    },
    dest: {
      sql: `${CUBE}.Dest`,
      type: `string`
    },
    destcityname: {
      sql: `${CUBE}.DestCityName`,
      type: `string`
    },
    deststate: {
      sql: `${CUBE}.DestState`,
      type: `string`
    },
    deststatefips: {
      sql: `${CUBE}.DestStateFips`,
      type: `string`
    },
    deststatename: {
      sql: `${CUBE}.DestStateName`,
      type: `string`
    },
    depdelay: {
      sql: `${CUBE}.DepDelay`,
      type: `number`
    },
    departuredelaygroups: {
      sql: `${CUBE}.DepartureDelayGroups`,
      type: `string`
    },
    deptimeblk: {
      sql: `${CUBE}.DepTimeBlk`,
      type: `string`
    },
    arrtimeblk: {
      sql: `${CUBE}.ArrTimeBlk`,
      type: `string`
    },
    cancellationcode: {
      sql: `${CUBE}.CancellationCode`,
      type: `string`
    },
    firstdeptime: {
      sql: `${CUBE}.FirstDepTime`,
      type: `string`
    },
    totaladdgtime: {
      sql: `${CUBE}.TotalAddGTime`,
      type: `string`
    },
    longestaddgtime: {
      sql: `${CUBE}.LongestAddGTime`,
      type: `string`
    },
    flightdate: {
      sql: `${CUBE}.FlightDate`,
      type: `time`
    }
  }
});
