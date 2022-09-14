const Sequelize = require('sequelize')

var db = {}

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

const initModels = require("./models/init-models");
const models = initModels(sequelize);

db.models = models
db.sequelize = sequelize
db.Sequelize = Sequelize

module.exports = db


// sequelize-auto -h demo-db-examples.cube.dev -d fraud -u cube -x 12345 -p 5432  --dialect postgres -c ./sequelize-auto-settings -o ./models
