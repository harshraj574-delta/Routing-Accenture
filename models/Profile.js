const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

// Make sure sequelize is properly imported before using it
if (!sequelize) {
  console.error('Sequelize instance is undefined in Profile.js');
  process.exit(1);
}

const Profile = sequelize.define('Profile', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  name: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true
  },
  zoneClubbing: {
    type: DataTypes.BOOLEAN,
    allowNull: false,
    defaultValue: false
  },
  highCapacityZones: {
    type: DataTypes.JSON,
    allowNull: false,
    defaultValue: []
  },
  zonePairingMatrix: {
    type: DataTypes.JSON,
    allowNull: false,
    defaultValue: {}
  },
  isAutoClubbing: {
    type: DataTypes.BOOLEAN,
    allowNull: false,
    defaultValue: false
  },
  createdAt: {
    type: DataTypes.DATE,
    defaultValue: DataTypes.NOW
  },
  updatedAt: {
    type: DataTypes.DATE,
    defaultValue: DataTypes.NOW
  }
}, {
  tableName: 'profiles'
});

module.exports = Profile;