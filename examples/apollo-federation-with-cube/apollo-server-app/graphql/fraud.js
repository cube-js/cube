const { gql } = require('apollo-server-express')
const db = require('../database')

const amountSumFraudsWithStep = ({ isFraud, stepStart, stepEnd }) => `
  SELECT
    "fraud"."isFraud" as isfraud,
    "fraud"."step" as step,
    "fraud"."type" as type,
    sum("fraud"."amount") as amountsum
  FROM public.fraud AS "fraud"
  WHERE
    "fraud"."isFraud" = ${isFraud} 
      AND 
    "fraud"."step" BETWEEN ${stepStart} AND ${stepEnd}
  GROUP BY 1, 2, 3
  ORDER BY 1 ASC;
`

module.exports = {
  typeDefs: gql`
    extend type Query {
      fraudsByAmountSumWithStep(isFraud: Int, stepStart: Int, stepEnd: Int): [Fraud]
    }

    type Fraud {
      id: ID!
      step: Float
      type: String
      isFraud: Int
      amountsum: Float
    }
  `,
  resolvers: {
    Query: {
      fraudsByAmountSumWithStep: async (obj, args, context, info) =>
        db.sequelize.query(
          amountSumFraudsWithStep(
            { isFraud: args.isFraud, stepStart: args.stepStart, stepEnd: args.stepEnd }
          ), 
          { type: db.sequelize.QueryTypes.SELECT }
        ),
    },
  }
}
