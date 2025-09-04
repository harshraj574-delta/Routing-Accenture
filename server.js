const express = require('express');
const cors = require('cors');
require('dotenv').config();

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
const routeGenerationRoutes = require('./routes/routeGenerationRoutes');

// Add this right before you define your routes
console.log('----- Available route files -----');
console.log('routeGenerationRoutes exists:', !!routeGenerationRoutes);

// Define routes with explicit paths
app.use('/api/route-generation', routeGenerationRoutes);

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// After defining routes, log what's registered
console.log('----- Registered API routes -----');
console.log('/api/employees registered');
console.log('/api/profiles registered');
console.log('/api/routes registered');
console.log('/api/facilities registered');
console.log('/api/route-generation/* registered (POST endpoints)');


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

    app.listen(PORT,() => {
      console.log(`Server running on port ${PORT}`);
    });

module.exports = { app };