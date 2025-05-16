const express = require('express');
const router = express.Router();
const routeController = require('../controllers/routeController');

// Debug route to check if this file is being loaded
router.get('/test', (req, res) => {
  res.json({ message: 'Route routes test endpoint working!' });
});

// Get all routes
router.get('/', routeController.getAll);

// Create new routes for a profile
router.post('/', routeController.create);

// Get routes by date and shift
router.get('/by-date-shift', routeController.getByDateAndShift);

// Get routes by profile ID
router.get('/profile/:profileId', routeController.getByProfile);

// Get route by ID
router.get('/:id', routeController.getById);

module.exports = router;