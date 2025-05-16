const sequelize = require('../config/database');
const { DataTypes } = require('sequelize');

// Import models individually 
const Profile = require('./Profile');
const Route = require('./Route');
const RouteLeg = require('./RouteLeg'); // Import the new RouteLeg model
// Import other models
const EmployeeModel = require('./Employee');
const FacilityModel = require('./Facility');

// Initialize models that require sequelize instance
const Employee = EmployeeModel(sequelize);
const Facility = FacilityModel(sequelize);

// Define associations
Profile.hasMany(Route, { foreignKey: 'ProfileId', as: 'routes' });
Route.belongsTo(Profile, { foreignKey: 'ProfileId', as: 'profileDetails' });

// Association: A Route (batch) has many RouteLegs
Route.hasMany(RouteLeg, { 
  foreignKey: 'routeUuid', 
  sourceKey: 'uuid', // Link using the uuid field on the Route model
  as: 'legs' // Alias to use when querying
});
// Association: A RouteLeg belongs to one Route (batch)
RouteLeg.belongsTo(Route, { 
  foreignKey: 'routeUuid', 
  targetKey: 'uuid' // Link using the uuid field on the Route model
});

// Keep force: false for safety in production/persistent data scenarios
// Use { alter: true } during development if you want Sequelize to attempt non-destructive updates
// Use { force: true } ONLY if you intend to drop and recreate tables (DATA LOSS)
sequelize.sync({ force: false }) 
  .then(() => {
    console.log('Database synced successfully based on model definitions.');
    console.log('Remember: force:false does NOT automatically migrate schema changes like adding/removing columns or tables.');
    console.log('Manual migration or force:true (caution!) or alter:true might be needed for schema updates.');
  })
  .catch(err => {
    console.error('Error syncing database:', err);
  });

// Export all models together
module.exports = {
  sequelize,
  Profile,
  Route,
  RouteLeg, // Export the new model
  Employee,
  Facility
  // Include other models here
};