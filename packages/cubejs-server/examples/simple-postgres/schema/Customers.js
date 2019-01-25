cube(`Customers`, {
  sql: `select * from bots`,

  joins: {
    
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [city, country]
    },

    customerNumber: {
      sql: `${CUBE}.\`customerNumber\``,
      type: `sum`
    },

    salesRepEmployeeNumber: {
      sql: `${CUBE}.\`salesRepEmployeeNumber\``,
      type: `sum`
    }
  },

  dimensions: {
    addressLine2: {
      sql: `${CUBE}.\`addressLine2\``,
      type: `string`
    },

    contactLastName: {
      sql: `${CUBE}.\`contactLastName\``,
      type: `string`
    },

    contactFirstName: {
      sql: `${CUBE}.\`contactFirstName\``,
      type: `string`
    },

    phone: {
      sql: `phone`,
      type: `string`
    },

    customerName: {
      sql: `${CUBE}.\`customerName\``,
      type: `string`
    },

    addressLine1: {
      sql: `${CUBE}.\`addressLine1\``,
      type: `string`
    },

    city: {
      sql: `city`,
      type: `string`
    },

    state: {
      sql: `state`,
      type: `string`
    },

    postalCode: {
      sql: `${CUBE}.\`postalCode\``,
      type: `string`
    },

    country: {
      sql: `country`,
      type: `string`
    }
  }
});