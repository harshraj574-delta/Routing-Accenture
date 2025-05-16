const sequelize = require('./config/database');

async function checkDatabase() {
  try {
    console.log('Connecting to database...');
    await sequelize.authenticate();
    console.log('Connection successful!');
    
    // Show all tables
    const [tables] = await sequelize.query('SHOW TABLES');
    console.log('Tables in database:', tables);
    
    // Check if routes table exists
    const routesTable = tables.find(table => 
      Object.values(table)[0].toLowerCase() === 'routes'
    );
    
    if (routesTable) {
      console.log('Routes table exists!');
      // Show table structure
      const [columns] = await sequelize.query('DESCRIBE routes');
      console.log('Routes table structure:', columns);
      
      // Count records
      const [count] = await sequelize.query('SELECT COUNT(*) as count FROM routes');
      console.log('Number of records in routes table:', count[0].count);
      
      // Show sample routes
      if (count[0].count > 0) {
        const [routes] = await sequelize.query('SELECT * FROM routes LIMIT 5');
        console.log('Sample routes:', routes);
      }
    } else {
      console.log('Routes table does not exist!');
    }
  } catch (error) {
    console.error('Database check failed:', error);
  } finally {
    process.exit();
  }
}

checkDatabase(); 