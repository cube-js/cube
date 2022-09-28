cube(`Ontime`, {
  sql: `SELECT * FROM default.ontime`,
  dataSource: `clickhouse`,

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
    avgDepDelay: {
      type: `avg`,
      sql: `${CUBE}."DepDelay"`
    },
    avgDepDelayGreaterThanTenMinutesPercentage: {
      type: `number`,
      sql: `avg(${CUBE}."DepDelay">10)*100`,
      format: `percent`
    },
  },
  
  dimensions: {
    year: {
      sql: `${CUBE}."Year"`,
      type: `number`
    },
    quarter: {
      sql: `${CUBE}."Quarter"`,
      type: `number`
    },
    month: {
      sql: `${CUBE}."Month"`,
      type: `number`
    },
    dayofmonth: {
      sql: `${CUBE}."DayofMonth"`,
      type: `number`
    },
    dayofweek: {
      sql: `${CUBE}."DayOfWeek"`,
      type: `number`
    },
    airlineid: {
      sql: `${CUBE}."AirlineID"`,
      type: `number`
    },
    




    uniquecarrier: {
      sql: `${CUBE}."UniqueCarrier"`,
      type: `string`
    },
    
    carrier: {
      sql: `${CUBE}."Carrier"`,
      type: `string`
    },
    
    tailnum: {
      sql: `${CUBE}."TailNum"`,
      type: `string`
    },
    
    flightnum: {
      sql: `${CUBE}."FlightNum"`,
      type: `string`
    },
    
    origin: {
      sql: `${CUBE}."Origin"`,
      type: `string`
    },
    
    origincityname: {
      sql: `${CUBE}."OriginCityName"`,
      type: `string`
    },
    
    originstate: {
      sql: `${CUBE}."OriginState"`,
      type: `string`
    },
    
    originstatefips: {
      sql: `${CUBE}."OriginStateFips"`,
      type: `string`
    },
    
    originstatename: {
      sql: `${CUBE}."OriginStateName"`,
      type: `string`
    },
    
    dest: {
      sql: `${CUBE}."Dest"`,
      type: `string`
    },
    
    destcityname: {
      sql: `${CUBE}."DestCityName"`,
      type: `string`
    },
    
    deststate: {
      sql: `${CUBE}."DestState"`,
      type: `string`
    },
    
    deststatefips: {
      sql: `${CUBE}."DestStateFips"`,
      type: `string`
    },
    
    deststatename: {
      sql: `${CUBE}."DestStateName"`,
      type: `string`
    },
    
    depdelay: {
      sql: `${CUBE}."DepDelay"`,
      type: `number`
    },

    departuredelaygroups: {
      sql: `${CUBE}."DepartureDelayGroups"`,
      type: `string`
    },
    
    deptimeblk: {
      sql: `${CUBE}."DepTimeBlk"`,
      type: `string`
    },
    
    arrtimeblk: {
      sql: `${CUBE}."ArrTimeBlk"`,
      type: `string`
    },
    
    cancellationcode: {
      sql: `${CUBE}."CancellationCode"`,
      type: `string`
    },
    
    firstdeptime: {
      sql: `${CUBE}."FirstDepTime"`,
      type: `string`
    },
    
    totaladdgtime: {
      sql: `${CUBE}."TotalAddGTime"`,
      type: `string`
    },
    
    longestaddgtime: {
      sql: `${CUBE}."LongestAddGTime"`,
      type: `string`
    },
    
    divairportlandings: {
      sql: `${CUBE}."DivAirportLandings"`,
      type: `string`
    },
    
    divreacheddest: {
      sql: `${CUBE}."DivReachedDest"`,
      type: `string`
    },
    
    divactualelapsedtime: {
      sql: `${CUBE}."DivActualElapsedTime"`,
      type: `string`
    },
    
    divarrdelay: {
      sql: `${CUBE}."DivArrDelay"`,
      type: `string`
    },
    
    divdistance: {
      sql: `${CUBE}."DivDistance"`,
      type: `string`
    },
    
    div1airport: {
      sql: `${CUBE}."Div1Airport"`,
      type: `string`
    },
    
    div1wheelson: {
      sql: `${CUBE}."Div1WheelsOn"`,
      type: `string`
    },
    
    div1totalgtime: {
      sql: `${CUBE}."Div1TotalGTime"`,
      type: `string`
    },
    
    div1longestgtime: {
      sql: `${CUBE}."Div1LongestGTime"`,
      type: `string`
    },
    
    div1wheelsoff: {
      sql: `${CUBE}."Div1WheelsOff"`,
      type: `string`
    },
    
    div1tailnum: {
      sql: `${CUBE}."Div1TailNum"`,
      type: `string`
    },
    
    div2airport: {
      sql: `${CUBE}."Div2Airport"`,
      type: `string`
    },
    
    div2wheelson: {
      sql: `${CUBE}."Div2WheelsOn"`,
      type: `string`
    },
    
    div2totalgtime: {
      sql: `${CUBE}."Div2TotalGTime"`,
      type: `string`
    },
    
    div2longestgtime: {
      sql: `${CUBE}."Div2LongestGTime"`,
      type: `string`
    },
    
    div2wheelsoff: {
      sql: `${CUBE}."Div2WheelsOff"`,
      type: `string`
    },
    
    div2tailnum: {
      sql: `${CUBE}."Div2TailNum"`,
      type: `string`
    },
    
    div3airport: {
      sql: `${CUBE}."Div3Airport"`,
      type: `string`
    },
    
    div3wheelson: {
      sql: `${CUBE}."Div3WheelsOn"`,
      type: `string`
    },
    
    div3totalgtime: {
      sql: `${CUBE}."Div3TotalGTime"`,
      type: `string`
    },
    
    div3longestgtime: {
      sql: `${CUBE}."Div3LongestGTime"`,
      type: `string`
    },
    
    div3wheelsoff: {
      sql: `${CUBE}."Div3WheelsOff"`,
      type: `string`
    },
    
    div3tailnum: {
      sql: `${CUBE}."Div3TailNum"`,
      type: `string`
    },
    
    div4airport: {
      sql: `${CUBE}."Div4Airport"`,
      type: `string`
    },
    
    div4wheelson: {
      sql: `${CUBE}."Div4WheelsOn"`,
      type: `string`
    },
    
    div4totalgtime: {
      sql: `${CUBE}."Div4TotalGTime"`,
      type: `string`
    },
    
    div4longestgtime: {
      sql: `${CUBE}."Div4LongestGTime"`,
      type: `string`
    },
    
    div4wheelsoff: {
      sql: `${CUBE}."Div4WheelsOff"`,
      type: `string`
    },
    
    div4tailnum: {
      sql: `${CUBE}."Div4TailNum"`,
      type: `string`
    },
    
    div5airport: {
      sql: `${CUBE}."Div5Airport"`,
      type: `string`
    },
    
    div5wheelson: {
      sql: `${CUBE}."Div5WheelsOn"`,
      type: `string`
    },
    
    div5totalgtime: {
      sql: `${CUBE}."Div5TotalGTime"`,
      type: `string`
    },
    
    div5longestgtime: {
      sql: `${CUBE}."Div5LongestGTime"`,
      type: `string`
    },
    
    div5wheelsoff: {
      sql: `${CUBE}."Div5WheelsOff"`,
      type: `string`
    },
    
    div5tailnum: {
      sql: `${CUBE}."Div5TailNum"`,
      type: `string`
    },
    
    flightdate: {
      sql: `${CUBE}."FlightDate"`,
      type: `time`
    }
  }
});
