const express = require('express');
const router = express.Router();
const employeeController = require('../controllers/employeeController');

// Handle preflight requests
router.options('/', (req, res) => {
  res.header('Access-Control-Allow-Origin', 'http://localhost:5173');
  res.header('Access-Control-Allow-Methods', 'GET,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Accept');
  res.status(200).end();
});

// GET /api/employees
router.get('/', (req, res, next) => {
  console.log('Employee route hit with query:', req.query);
  employeeController.getAllEmployees(req, res).catch(next);
});

module.exports = router; 