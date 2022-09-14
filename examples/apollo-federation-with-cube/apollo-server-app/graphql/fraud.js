const { gql } = require('apollo-server-express')
const db = require('../database')

const amountSumFrauds = `
  SELECT
    "fraud"."isFraud" as isfraud,
    "fraud"."step" as step,
    "fraud"."type" as type,
    sum("fraud"."amount") as amountsum
  FROM public.fraud AS "fraud"
  WHERE "fraud"."isFraud" = 1
  GROUP BY 1, 2, 3
  ORDER BY 1 ASC;
`
const amountSumNonFrauds = `
  SELECT
    "fraud"."isFraud" as isfraud,
    "fraud"."step" as step,
    "fraud"."type" as type,
    sum("fraud"."amount") as amountsum
  FROM public.fraud AS "fraud"
  WHERE "fraud"."isFraud" = 0
  GROUP BY 1, 2, 3
  ORDER BY 1 ASC;
`

module.exports = {
  typeDefs: gql`
    extend type Query {
      frauds: [Fraud]
      fraudsByAmountSum: [Fraud]
      nonFraudsByAmountSum: [Fraud]
    }

    type Fraud {
      id: ID!
      step: Float
      type: String
      amount: Float
      nameOrig: String
      oldbalanceOrg: Float
      newbalanceOrg: Float
      nameDest: String
      oldbalanceDest: Float
      newbalanceDest: Float
      isFraud: Int
      isFlaggedFraud: Int

      amountsum: Float
    }
  `,
  resolvers: {
    Query: {
      frauds: async () => db.models.fraud.findAll(),
      fraudsByAmountSum: async () => db.sequelize.query(amountSumFrauds, { type: db.sequelize.QueryTypes.SELECT }),
      nonFraudsByAmountSum: async () => db.sequelize.query(amountSumNonFrauds, { type: db.sequelize.QueryTypes.SELECT }),
    },
  }
}
