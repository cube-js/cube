// Fraud.js

cube(`Fraud`, {
  sql: `SELECT * FROM public.fraud`,
  preAggregations: {
    main: {
      measures: [Fraud.amount, Fraud.count ],
      dimensions: [Fraud.type, Fraud.isfraud, Fraud.isflaggedfraud, Fraud.nameorig]
    }
  },
  joins: {},
  measures: {
    count: {
      type: `count`,
      drillMembers: [nameorig, namedest]
    },
    amount: {
      sql: `amount`,
      type: `sum`
    }
  },
  dimensions: {
    type: {
      sql: `type`,
      type: `string`
    },
    nameorig: {
      sql: `${CUBE}."nameOrig"`,
      type: `string`
    },
    oldbalanceorg: {
      sql: `${CUBE}."oldbalanceOrg"`,
      type: `string`
    },
    newbalanceorig: {
      sql: `${CUBE}."newbalanceOrig"`,
      type: `string`
    },
    namedest: {
      sql: `${CUBE}."nameDest"`,
      type: `string`
    },
    isfraud: {
      sql: `${CUBE}."isFraud"`,
      type: `string`
    },
    isflaggedfraud: {
      sql: `${CUBE}."isFlaggedFraud"`,
      type: `string`
    }
  }
});