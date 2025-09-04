const express = require('express');
const router = express.Router();
const routeGenerationController = require('../controllers/routeGenerationController');

// Debug route to check if this file is being loaded
router.get('/test', (req, res) => {
  res.json({ message: 'Route generation routes test endpoint working!' });
});

// Generate routes based on data in request body
router.post('/generate', routeGenerationController.generateRoutes);

// Recalculate routes based on data in request body
router.post('/recalculate', routeGenerationController.recalculateRoute);

// Simple GET endpoint for testing (returns info about the endpoint)
router.get('/recalculate', (req, res) => {
  res.json({ 
    message: 'This endpoint requires a POST request with data',
    method: 'POST',
    requiredBody: 'routes, facility, shiftTime, pickupTimePerEmployee, reportingTime, city, tripType'
  });
});

module.exports = router;