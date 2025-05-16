const express = require('express');
const router = express.Router();
const routeGenerationController = require('../controllers/routeGenerationController');

// Debug route to check if this file is being loaded
router.get('/test', (req, res) => {
  res.json({ message: 'Route generation routes test endpoint working!' });
});

// Generate routes based on data in request body
router.post('/generate', routeGenerationController.generateRoutes);

module.exports = router;