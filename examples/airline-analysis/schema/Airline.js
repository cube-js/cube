cube(`Airline`, {
  sql: `SELECT * FROM airline.airline`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [airportName, timeMonthName, statisticsCarriersNames]
    },
    
    statisticsCarriersTotal: {
      sql: `${CUBE}.\`Statistics_Carriers_Total\``,
      type: `sum`
    },
    
    statisticsFlightsTotal: {
      sql: `${CUBE}.\`Statistics_Flights_Total\``,
      type: `sum`
    },
    
    statisticsMinutesDelayedTotal: {
      sql: `${CUBE}.\`Statistics_Minutes_Delayed_Total\``,
      type: `sum`
    }
  },
  
  dimensions: {
    airportCode: {
      sql: `${CUBE}.\`Airport_Code\``,
      type: `string`
    },
    
    airportName: {
      sql: `${CUBE}.\`Airport_Name\``,
      type: `string`
    },
    
    timeLabel: {
      sql: `${CUBE}.\`Time_Label\``,
      type: `string`
    },
    
    timeMonthName: {
      sql: `${CUBE}.\`Time_Month_Name\``,
      type: `string`
    },
    
    statisticsCarriersNames: {
      sql: `${CUBE}.\`Statistics_Carriers_Names\``,
      type: `string`
    }
  },
  
  dataSource: `default`
});
