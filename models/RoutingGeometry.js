// const { DataTypes } = require('sequelize');
// const sequelize = require('../config/database');

// const RoutingGeometry = sequelize.define('RoutingGeometry', {
//   id: {
//     type: DataTypes.INTEGER,
//     primaryKey: true,
//     autoIncrement: true
//   },
//   routeUuid: {
//     type: DataTypes.STRING, // Changed from UUID to STRING to match Route.uuid
//     allowNull: false,
//     references: {
//       model: 'Routes', // Ensure this matches the table name generated by Sequelize for the Route model
//       key: 'uuid'
//     },
//     onUpdate: 'CASCADE',
//     onDelete: 'CASCADE'
//   },
//   geometry: {
//     type: DataTypes.JSON, // Using JSON to store geometry object
//     allowNull: false
//   },
//   // You might want to add other relevant fields per individual route leg, e.g.,
//   // routeIndex: DataTypes.INTEGER, // To maintain order if needed
//   // employeeIds: DataTypes.JSON, // If you want to store which employees are on this specific leg geometry
//   // distance: DataTypes.FLOAT,
//   // duration: DataTypes.FLOAT,
// });

// module.exports = RoutingGeometry;
