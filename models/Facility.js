// const { DataTypes } = require('sequelize');

// module.exports = (sequelize) => {
//   const Facility = sequelize.define('Facility', {
//     id: {
//       type: DataTypes.INTEGER,
//       primaryKey: true,
//       autoIncrement: true,
//       field: 'id'
//     },
//     name: {
//       // type: DataTypes.STRING(100),
//       allowNull: false,
//       unique: true,
//       field: 'name'
//     },
//     geoX: {
//       type: DataTypes.FLOAT,
//       allowNull: false,
//       field: 'geoX'
//     },
//     geoY: {
//       type: DataTypes.FLOAT,
//       allowNull: false,
//       field: 'geoY'
//     },
//     createdAt: {
//       type: DataTypes.DATE,
//       defaultValue: DataTypes.NOW,
//       field: 'created_at'
//     }
//   }, {
//     tableName: 'facilities',
//     timestamps: true,
//     createdAt: 'created_at',
//     updatedAt: false,
//     freezeTableName: true
//   });

//   return Facility;
// }; 