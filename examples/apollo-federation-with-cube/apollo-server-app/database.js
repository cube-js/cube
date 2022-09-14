const Sequelize = require('sequelize')

const db = {}
const sequelize = new Sequelize(
  'fraud',
  'cube',
  '12345',
  {
    host: 'demo-db-examples.cube.dev',
    port: '5432',
    dialect: 'postgres',
    define: {
      freezeTableName: true,
    },
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
    operatorsAliases: false,
  }
)

db.sequelize = sequelize
db.Sequelize = Sequelize

module.exports = db
