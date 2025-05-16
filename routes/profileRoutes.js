const express = require('express');
const router = express.Router();
const profileController = require('../controllers/profileController');

// Create a new profile
router.post('/', profileController.create);

// Get all profiles
router.get('/', profileController.getAll);

// Get profile by ID
router.get('/:id', profileController.getById);

// Update profile
router.put('/:id', profileController.update);

// Delete profile
router.delete('/:id', profileController.delete);

module.exports = router;