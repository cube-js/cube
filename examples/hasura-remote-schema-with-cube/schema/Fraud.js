cube(`Fraud`, {
  sql: `SELECT * FROM public.fraud`,
  
  preAggregations: {
    fraudAmountSum: {
      measures: [
        Fraud.amountSum
      ],
      dimensions: [
        Fraud.isFraud,
        Fraud.step,
        Fraud.type
      ]
    }
  },
  joins: {},
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [nameorig, namedest]
    },
    amountSum: {
      sql: `${CUBE}."amount"`,
      type: `sum`,
    },
  },
  
  dimensions: {
    step: {
      sql: `${CUBE}."step"`,
      type: `string`
    },
    
    newbalancedest: {
      sql: `${CUBE}."newbalanceDest"`,
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
    
    namedest: {
      sql: `${CUBE}."nameDest"`,
      type: `string`
    },
    
    newbalanceorg: {
      sql: `${CUBE}."newbalanceOrg"`,
      type: `string`
    },
    
    oldbalancedest: {
      sql: `${CUBE}."oldbalanceDest"`,
      type: `string`
    },
    
    type: {
      sql: `${CUBE}."type"`,
      type: `string`
    },
    
    amount: {
      sql: `${CUBE}."amount"`,
      type: `number`
    },

    isFraud: {
      sql: `${CUBE}."isFraud"`,
      type: `boolean`
    },

    isFlaggedFraud: {
      sql: `${CUBE}."isFlaggedFraud"`,
      type: `boolean`
    }
  }
});
