// const { DataTypes } = require('sequelize');

// module.exports = (sequelize) => {
//   const Employee = sequelize.define('Employee', {
//     empCode: {
//       type: DataTypes.STRING(50),
//       primaryKey: true,
//       field: 'empCode'
//     },
//     address: {
//       type: DataTypes.TEXT,
//       allowNull: true,
//       field: 'address'
//     },
//     geoX: {
//       type: DataTypes.FLOAT,
//       allowNull: true,
//       field: 'geoX'
//     },
//     geoY: {
//       type: DataTypes.FLOAT,
//       allowNull: true,
//       field: 'geoY'
//     },
//     shift: {
//       type: DataTypes.INTEGER,
//       allowNull: true,
//       field: 'shift'
//     },
//     gender: {
//       type: DataTypes.CHAR(1),
//       allowNull: true,
//       field: 'gender'
//     }
//   }, {
//     tableName: 'employeedet',
//     timestamps: false,
//     freezeTableName: true
//   });

//   return Employee;
// }; 