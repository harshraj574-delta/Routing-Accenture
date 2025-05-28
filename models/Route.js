// const { DataTypes } = require('sequelize');
// const sequelize = require('../config/database');

// const Route = sequelize.define('Route', {
//   id: {
//     type: DataTypes.INTEGER,
//     primaryKey: true,
//     autoIncrement: true
//   },
//   uuid: {
//     type: DataTypes.STRING,
//     unique: true
//   },
//   date: {
//     type: DataTypes.DATEONLY,
//     allowNull: false
//   },
//   shift: {
//     type: DataTypes.STRING,
//     allowNull: false
//   },
//   tripType: {
//     type: DataTypes.STRING,
//     allowNull: true
//   },
//   facilityId: {
//     type: DataTypes.INTEGER,
//     allowNull: true
//   },
//   facility: {
//     type: DataTypes.JSON,
//     allowNull: true
//   },
//   profile: {
//     type: DataTypes.JSON,
//     allowNull: true
//   },
//   employeeData: {
//     type: DataTypes.JSON,
//     allowNull: true
//   },
//   // routeData column removed as geometry is now stored in RoutingGeometry table
//   totalEmployees: {
//     type: DataTypes.INTEGER,
//     defaultValue: 0
//   },
//   totalRoutes: {
//     type: DataTypes.INTEGER,
//     defaultValue: 0
//   },
//   averageOccupancy: {
//     type: DataTypes.FLOAT,
//     defaultValue: 0
//   },
//   ProfileId: {
//     type: DataTypes.INTEGER,
//     allowNull: true
//   },
//   createdAt: {
//     type: DataTypes.DATE,
//     defaultValue: DataTypes.NOW
//   },
//   updatedAt: {
//     type: DataTypes.DATE,
//     defaultValue: DataTypes.NOW
//   },
//   consolidatedPolyline: {
//     type: DataTypes.TEXT,
//     allowNull: true,
//     comment: 'Consolidated encoded polyline for the entire route'
//   },
//   routeLegsData: {
//     type: DataTypes.JSON,
//     allowNull: true,
//     comment: 'Consolidated route legs data including individual polylines and details'
//   },
//   routeDetails: {
//     type: DataTypes.JSON,
//     allowNull: true,
//     comment: 'Additional route details like total distance, duration, and waypoints'
//   }
// }, {
//   tableName: 'routes'
// });

// module.exports = Route;