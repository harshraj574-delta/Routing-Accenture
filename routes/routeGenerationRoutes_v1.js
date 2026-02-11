/**
 * Route Generation Routes - API Version 1 (Legacy)
 * Endpoint: /api/v1/route-generation/*
 */

const express = require('express');
const router = express.Router();
const routeGenerationController = require('../controllers/routeGenerationController_v1');

// Debug route to check if this file is being loaded
router.get('/test', (req, res) => {
    res.json({
        message: 'Route generation v1 (legacy) test endpoint working!',
        apiVersion: 'v1',
        description: 'This uses the original routing algorithm'
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

// Simple GET endpoint for testing (returns info about the endpoint)
router.get('/recalculate', (req, res) => {
    res.json({
        message: 'This endpoint requires a POST request with data',
        apiVersion: 'v1',
        method: 'POST',
        requiredBody: 'routes, facility, shiftTime, pickupTimePerEmployee, reportingTime, city, tripType'
    });
});

module.exports = router;
