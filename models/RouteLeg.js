const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

const RouteLeg = sequelize.define('RouteLeg', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  routeUuid: { // Foreign key to link to the main Route (batch)
    type: DataTypes.STRING, // Matches Route.uuid type
    allowNull: false,
    references: {
      model: 'routes', // Ensure this matches the table name for the Route model
      key: 'uuid'
    },
    onUpdate: 'CASCADE',
    onDelete: 'CASCADE'
  },
  legIndex: { // To maintain the order of legs within a batch
    type: DataTypes.INTEGER,
    allowNull: false
  },
  encodedPolyline: { // Store the OSRM encoded polyline string
    type: DataTypes.TEXT,
    allowNull: true // Allow null if OSRM fails for a leg
  },
  distance: { // Distance of this leg in meters
    type: DataTypes.FLOAT,
    allowNull: true
  },
  duration: { // Duration of this leg in seconds
    type: DataTypes.FLOAT,
    allowNull: true
  },
  employees: { // JSON array of employee objects specific to this leg
    type: DataTypes.JSON,
    allowNull: true
  },
  // Add other per-leg details if needed, e.g., start/end coordinates if useful
  // startCoords: { type: DataTypes.JSON, allowNull: true },
  // endCoords: { type: DataTypes.JSON, allowNull: true },
  createdAt: {
    type: DataTypes.DATE,
    defaultValue: DataTypes.NOW
  },
  updatedAt: {
    type: DataTypes.DATE,
    defaultValue: DataTypes.NOW
  }
}, {
  tableName: 'route_legs', // Explicit table name
  indexes: [
    { // Index for faster lookup by routeUuid and order
      fields: ['routeUuid', 'legIndex']
    }
  ]
});

module.exports = RouteLeg;
