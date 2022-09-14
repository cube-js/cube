const DataTypes = require("sequelize").DataTypes;
const _fraud = require("./fraud");

function initModels(sequelize) {
  const fraud = _fraud(sequelize, DataTypes);


  return {
    fraud,
  };
}
module.exports = initModels;
module.exports.initModels = initModels;
module.exports.default = initModels;
