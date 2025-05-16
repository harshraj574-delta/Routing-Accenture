const express = require('express');
const router = express.Router();
const facilityController = require('../controllers/facilityController');

// Get all facilities
router.get('/', facilityController.getAllFacilities);

// Create a new facility
router.post('/', facilityController.createFacility);

// Get a single facility
router.get('/:id', facilityController.getFacilityById);

// Update a facility
router.put('/:id', facilityController.updateFacility);

// Delete a facility
router.delete('/:id', facilityController.deleteFacility);

module.exports = router; 