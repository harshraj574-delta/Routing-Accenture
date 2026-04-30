/**
 * Features in v2:
 * - Angular sector clustering for linear routes
 * - Singleton aggregation to reduce single-employee routes
 * - Linear direction optimization (prevents zig-zagging)
 * - Improved vehicle utilization
 */

const express = require('express');
const router = express.Router();
const routeGenerationController = require('../controllers/routeGenerationController_v2');

// Debug route to check if this file is being loaded
router.get('/test', (req, res) => {
    res.json({
        message: 'Route generation v2 (enhanced) test endpoint working!',
        apiVersion: 'v2',
        description: 'This uses the enhanced routing algorithm with angular clustering',
        improvements: [
            'Angular sector clustering for linear routes',
            'Singleton aggregation to reduce single-employee routes',
            'Linear direction optimization (prevents zig-zagging)',
            'Improved vehicle utilization'
        ]
    });
});

// Generate routes based on data in request body
router.post('/generate', routeGenerationController.generateRoutes);

// Recalculate routes based on data in request body
router.post('/recalculate', routeGenerationController.recalculateRoute);

// New endpoint to get ETA between two points
router.post('/eta', routeGenerationController.getEta);

// Reoptimize route - uses VRP solver to find optimal stop order before calculating ETA/polyline
router.post('/reoptimize', routeGenerationController.reoptimizeRoute);

// Async route generation — returns jobId immediately, runs computation in background
router.post('/generate/async', routeGenerationController.generateRoutesAsync);

// Poll a specific job's status
router.get('/jobs/:jobId', routeGenerationController.getJobStatus);

// Check if a generation is already running for a given shift (cross-admin visibility)
// Query params: facilityid, sDate, triptype
router.get('/in-progress', routeGenerationController.checkInProgress);

// Simple GET endpoint for testing (returns info about the endpoint)
router.get('/recalculate', (req, res) => {
    res.json({
        message: 'This endpoint requires a POST request with data',
        apiVersion: 'v2',
        method: 'POST',
        requiredBody: 'routes, facility, shiftTime, pickupTimePerEmployee, reportingTime, city, tripType'
    });
});

module.exports = router;
