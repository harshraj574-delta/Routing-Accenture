/**
 * Test script for route generation service
 * 
 * This script tests the route generation service with sample data
 * to verify that all employees are properly assigned to routes.
 */

const routeGenerationService = require('./services/routeGenerationService');

// Sample data from user's request
const testData = {
  "employees": [
    {
      "empCode": "EMP001",
      "address": "B-45 Kalkaji, New Delhi",
      "geoX": 77.2571,
      "geoY": 28.5490,
      "shift": 1230,
      "gender": "M"
    },
    {
      "empCode": "EMP002",
      "address": "C-12 Sarai Kale Khan, New Delhi",
      "geoX": 77.2520,
      "geoY": 28.5479,
      "shift": 1230,
      "gender": "F"
    },
    {
      "empCode": "EMP003",
      "address": "H-78 Lajpat Nagar, New Delhi",
      "geoX": 77.2400,
      "geoY": 28.5700,
      "shift": 1230,
      "gender": "M"
    },
    {
      "empCode": "EMP004",
      "address": "A-15 Greater Kailash, New Delhi",
      "geoX": 77.2350,
      "geoY": 28.5420,
      "shift": 1230,
      "gender": "F"
    },
    {
      "empCode": "EMP005",
      "address": "D-22 Nehru Place, New Delhi",
      "geoX": 77.2500,
      "geoY": 28.5480,
      "shift": 1230,
      "gender": "M"
    },
    {
      "empCode": "EMP006",
      "address": "F-33 Okhla, New Delhi",
      "geoX": 77.2700,
      "geoY": 28.5400,
      "shift": 1230,
      "gender": "F"
    },
    {
      "empCode": "EMP007",
      "address": "G-56 Malviya Nagar, New Delhi",
      "geoX": 77.2100,
      "geoY": 28.5350,
      "shift": 1230,
      "gender": "M"
    },
    {
      "empCode": "EMP008",
      "address": "J-67 Hauz Khas, New Delhi",
      "geoX": 77.2000,
      "geoY": 28.5450,
      "shift": 1230,
      "gender": "F"
    },
    {
      "empCode": "EMP009",
      "address": "K-89 Vasant Kunj, New Delhi",
      "geoX": 77.1600,
      "geoY": 28.5250,
      "shift": 1230,
      "gender": "M"
    },
    {
      "empCode": "EMP010",
      "address": "L-90 Dwarka, New Delhi",
      "geoX": 77.0500,
      "geoY": 28.5800,
      "shift": 1230,
      "gender": "F"
    }
  ],
  "facility": {
    "id": 1,
    "name": "Gurugram Facility",
    "geoX": 77.0200,
    "geoY": 28.4900
  },
  "shiftTime": "1230",
  "date": "2023-07-15",
  "profile": {
    "id": 1,
    "name": "Delhi NCR Profile",
    "zoneClubbing": true,
    "highCapacityZones": ["KALKAJI", "SARAI KALE KHAN"],
    "zonePairingMatrix": {
      "KALKAJI": ["SARAI KALE KHAN", "PALAM VIHAR"],
      "SARAI KALE KHAN": ["KALKAJI", "GGN SEC 4"],
      "PALAM VIHAR": ["KALKAJI", "GGN SEC 4"],
      "GGN SEC 4": ["SARAI KALE KHAN", "PALAM VIHAR"]
    },
    "isAutoClubbing": true
  },
  "saveToDatabase": false,
  "uuid": "test-uuid-" + Date.now()
};

// Mock the OSRM availability check to always return true
routeGenerationService.isOsrmAvailable = async () => true;

// Mock the calculateRouteDetails function to return dummy data
routeGenerationService.calculateRouteDetails = async (coordinates, employees) => {
  return {
    employees: employees.map((emp, index) => ({
      ...emp,
      duration: 300,
      distance: 1000,
      order: index + 1
    })),
    totalDistance: employees.length * 1000,
    totalDuration: employees.length * 300,
    encodedPolyline: "dummy_polyline",
    legs: employees.map(() => ({ duration: 300, distance: 1000 })),
    geometry: {
      type: "LineString",
      coordinates: []
    }
  };
};

// Run the test
async function runTest() {
  try {
    console.log('Testing route generation with sample data...');
    console.log(`Input: ${testData.employees.length} employees`);
    
    const result = await routeGenerationService.generateRoutes(testData);
    
    console.log('\nRoute generation result:');
    console.log(`Total employees: ${result.totalEmployees}`);
    console.log(`Total routes: ${result.totalRoutes}`);
    console.log(`Average occupancy: ${result.averageOccupancy}`);
    
    // Check if all employees are assigned to routes
    const assignedEmployees = new Set();
    result.routes.forEach(route => {
      console.log(`\nRoute ${route.routeNumber} (${route.zone}):`);
      console.log(`Vehicle capacity: ${route.vehicleCapacity}`);
      console.log(`Employees: ${route.employees.length}`);
      
      route.employees.forEach(emp => {
        console.log(`  - ${emp.empCode} (order: ${emp.order})`);
        assignedEmployees.add(emp.empCode);
      });
    });
    
    console.log('\nSummary:');
    console.log(`Total employees in input: ${testData.employees.length}`);
    console.log(`Total employees assigned to routes: ${assignedEmployees.size}`);
    
    if (assignedEmployees.size === testData.employees.length) {
      console.log('SUCCESS: All employees were assigned to routes!');
    } else {
      console.log('ERROR: Not all employees were assigned to routes!');
      
      // Find which employees were not assigned
      const unassignedEmployees = testData.employees.filter(
        emp => !assignedEmployees.has(emp.empCode)
      );
      
      console.log('Unassigned employees:');
      unassignedEmployees.forEach(emp => {
        console.log(`  - ${emp.empCode} (${emp.address})`);
      });
    }
  } catch (error) {
    console.error('Test failed with error:', error);
  }
}

runTest();