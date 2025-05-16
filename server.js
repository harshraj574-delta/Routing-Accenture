const express = require('express');
const cors = require('cors');
require('dotenv').config();
const { sequelize } = require('./models'); // Import sequelize from models/index.js
const debugController = require('./controllers/debugController');

const app = express();

// Add CORS middleware
app.use(cors({
  origin: '*', // Allow all origins - modify this in production!
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// Middleware
app.use(express.json({ limit: '50mb' }));

// Import routes
const employeeRoutes = require('./routes/employeeRoutes');
const profileRoutes = require('./routes/profileRoutes');
const routeRoutes = require('./routes/routeRoutes');
const facilityRoutes = require('./routes/facilityRoutes');
const routeGenerationRoutes = require('./routes/routeGenerationRoutes');

// Add this right before you define your routes
console.log('----- Available route files -----');
console.log('employeeRoutes exists:', !!employeeRoutes);
console.log('profileRoutes exists:', !!profileRoutes);
console.log('routeRoutes exists:', !!routeRoutes);
console.log('facilityRoutes exists:', !!facilityRoutes);
console.log('routeGenerationRoutes exists:', !!routeGenerationRoutes);

// Define routes with explicit paths
app.use('/api/employees', employeeRoutes);
app.use('/api/profiles', profileRoutes);
app.use('/api/routes', routeRoutes);
app.use('/api/facilities', facilityRoutes);
app.use('/api/route-generation', routeGenerationRoutes);

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Debug routes endpoint
app.get('/api/debug/routes', debugController.getRoutes);

// After your existing routes and before the error handlers
app.get('/api/debug', (req, res) => {
  res.json({
    routes: app._router.stack
      .filter(r => r.route)
      .map(r => ({
        path: r.route.path,
        methods: Object.keys(r.route.methods)
      }))
  });
});

// Add a simple test endpoint directly in server.js
app.get('/api/test', (req, res) => {
  res.json({ message: 'Test endpoint works!' });
});

// Add this direct route to check if the issue is with the router
app.get('/api/directroutes', async (req, res) => {
  try {
    const Route = require('./models/Route');
    const routes = await Route.findAll({
      order: [['createdAt', 'DESC']]
    });
    res.json(routes);
  } catch (error) {
    console.error('Error fetching routes directly:', error);
    res.status(500).json({ error: 'Failed to fetch routes directly' });
  }
});

// After defining routes, log what's registered
console.log('----- Registered API routes -----');
console.log('/api/employees registered');
console.log('/api/profiles registered');
console.log('/api/routes registered');
console.log('/api/facilities registered');

// Add this near your other routes
// app.get('/api/debug/database', async (req, res) => {
//   try {
//     // Test database connection
//     await sequelize.authenticate();
    
//     // Check routes table
//     const routes = await sequelize.query('SELECT * FROM routes LIMIT 5', { type: sequelize.QueryTypes.SELECT });
    
//     // Return database info
//     res.json({
//       status: 'Database connected',
//       tablesInfo: {
//         routes: {
//           count: routes.length,
//           sample: routes
//         }
//       }
//     });
//   } catch (error) {
//     console.error('Database debug error:', error);
//     res.status(500).json({
//       status: 'Database error',
//       error: error.message,
//       stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
//     });
//   }
// });

// Error handling
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

// Handle 404 - THIS SHOULD BE LAST
app.use((req, res) => {
  console.log('404 for URL:', req.url);
  res.status(404).json({ error: 'Not found' });
});

// Before starting the server
const PORT = process.env.PORT || 5001;

// Test database connection and sync models before starting server
sequelize.sync({ alter: false })
  .then(() => {
    console.log('Database synced');
    app.listen(PORT,() => {
      console.log(`Server running on port ${PORT}`);
    });
  })
  .catch(err => {
    console.error('Failed to sync database:', err);
  });

module.exports = { app };