module.exports = function(sequelize, DataTypes) {
  return sequelize.define('fraud', {
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      allowNull: false
    },
    step: {
      type: DataTypes.DECIMAL,
      allowNull: true
    },
    type: {
      type: DataTypes.STRING(25),
      allowNull: true
    },
    amount: {
      type: DataTypes.DECIMAL,
      allowNull: true
    },
    nameOrig: {
      type: DataTypes.STRING(25),
      allowNull: true
    },
    oldbalanceOrg: {
      type: DataTypes.DECIMAL,
      allowNull: true
    },
    newbalanceOrg: {
      type: DataTypes.DECIMAL,
      allowNull: true
    },
    nameDest: {
      type: DataTypes.STRING(25),
      allowNull: true
    },
    oldbalanceDest: {
      type: DataTypes.DECIMAL,
      allowNull: true
    },
    newbalanceDest: {
      type: DataTypes.DECIMAL,
      allowNull: true
    },
    isFraud: {
      type: DataTypes.SMALLINT,
      allowNull: false
    },
    isFlaggedFraud: {
      type: DataTypes.SMALLINT,
      allowNull: false
    }
  }, {
    sequelize,
    tableName: 'fraud',
    schema: 'public',
    timestamps: false
  });
};
