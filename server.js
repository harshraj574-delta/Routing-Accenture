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

// Import routes - Versioned API (Industry Standard)
const routeGenerationRoutes = require('./routes/routeGenerationRoutes');     // Default (points to v2)
const routeGenerationRoutesV1 = require('./routes/routeGenerationRoutes_v1'); // Legacy algorithm
const routeGenerationRoutesV2 = require('./routes/routeGenerationRoutes_v2'); // Enhanced algorithm

// Add this right before you define your routes
console.log('----- Available route files -----');
console.log('routeGenerationRoutes (default) exists:', !!routeGenerationRoutes);
console.log('routeGenerationRoutesV1 exists:', !!routeGenerationRoutesV1);
console.log('routeGenerationRoutesV2 exists:', !!routeGenerationRoutesV2);

// Define versioned API routes (Industry Standard Practice)
// v1 - Legacy routing algorithm (original)
app.use('/api/v1/route-generation', routeGenerationRoutesV1);
// v2 - Enhanced routing algorithm (with angular clustering, linear direction, singleton aggregation)
app.use('/api/v2/route-generation', routeGenerationRoutesV2);
// Default endpoint (backward compatible - uses new v2 algorithm)
app.use('/api/route-generation', routeGenerationRoutes);

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Version info endpoint
app.get('/api/versions', (req, res) => {
  res.json({
    availableVersions: ['v1', 'v2'],
    defaultVersion: 'v2',
    endpoints: {
      v1: {
        base: '/api/v1/route-generation',
        description: 'Legacy routing algorithm (original)',
        endpoints: [
          'POST /generate - Generate routes',
          'POST /recalculate - Recalculate existing routes',
          'POST /eta - Get ETA between points',
          'GET /test - Test endpoint'
        ]
      },
      v2: {
        base: '/api/v2/route-generation',
        description: 'Enhanced routing with angular clustering & linear direction optimization',
        endpoints: [
          'POST /generate - Generate routes',
          'POST /recalculate - Recalculate existing routes',
          'POST /eta - Get ETA between points',
          'GET /test - Test endpoint'
        ],
        improvements: [
          'Angular sector clustering for linear routes',
          'Singleton aggregation to reduce single-employee routes',
          'Linear direction optimization (prevents zig-zagging)',
          'Improved vehicle utilization'
        ]
      },
      default: {
        base: '/api/route-generation',
        description: 'Default endpoint (backward compatible) - Uses v2 algorithm'
      }
    }
  });
});

// After defining routes, log what's registered
console.log('----- Registered API routes -----');
console.log('/api/v1/route-generation/* registered (Legacy Algorithm)');
console.log('/api/v2/route-generation/* registered (Enhanced Algorithm)');
console.log('/api/route-generation/* registered (Default - V2)');
console.log('/api/versions - Version info endpoint');
console.log('/api/health - Health check endpoint');


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

const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);

  if (process.env.DB_SERVER) {
    const JobStore = require('./db/jobStore');
    // Fix any jobs that were mid-run when the server last stopped
    JobStore.markStuckJobsFailed()
      .then(() => JobStore.purgeOldJobs());
    // Purge completed/failed jobs older than 24 h every 6 hours
    setInterval(() => JobStore.purgeOldJobs(), 6 * 60 * 60 * 1000);
  }
});

// Allow long-running background jobs — only the HTTP response must complete within App Runner's
// 120 s Envoy timeout. Background setImmediate work is not subject to this limit.
server.setTimeout(0);

module.exports = { app };