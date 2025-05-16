const { Route, RouteLeg } = require('../models'); // Import Route and the new RouteLeg model
const { Op } = require('sequelize');
const { v4: uuidv4 } = require('uuid');

// Helper function to process a route with its legs for response
const processRouteWithLegs = (route) => {
  if (!route) return null;
  const routeObj = route.toJSON ? route.toJSON() : { ...route };

  // The associated legs are now in routeObj.legs
  // We rename/map 'legs' to 'routeData' for frontend compatibility
  // Ensure legs are sorted by legIndex for correct order
  routeObj.routeData = (routeObj.legs || [])
    .sort((a, b) => a.legIndex - b.legIndex)
    .map(leg => ({
      // Map leg data back to the structure expected by the frontend if needed
      // Assuming frontend expects: encodedPolyline, distance, duration, employees, etc.
      encodedPolyline: leg.encodedPolyline,
      distance: leg.distance,
      duration: leg.duration,
      employees: leg.employees, // Already JSON
      // Add other leg properties as needed
    }));
  delete routeObj.legs; // Remove the original legs array

  // Parse other top-level JSON fields on the Route object itself
  const fieldsToParse = ['profile', 'facility', 'employeeData', 'roadPathDetails', 'geometry', 'roadGeometry', 'routeDetails'];
  fieldsToParse.forEach(field => {
    if (routeObj[field] && typeof routeObj[field] === 'string') {
      try {
        routeObj[field] = JSON.parse(routeObj[field]);
      } catch (e) {
        console.warn(`Failed to parse ${field} JSON for route UUID ${routeObj.uuid}:`, e);
      }
    }
  });

  console.log(`Processed route ${routeObj.uuid} with ${routeObj.routeData?.length || 0} legs for response.`);
  return routeObj;
};

const routeController = {
  getAll: async (req, res) => {
    try {
      console.log('Fetching all routes with legs');
      const routes = await Route.findAll({
        include: [{ 
          model: RouteLeg, 
          as: 'legs' // Use the alias defined in the association
        }],
        order: [
          ['createdAt', 'DESC'],
          // Ensure legs are ordered correctly within each route
          // Note: This ordering might be better handled in processRouteWithLegs
          // [{ model: RouteLeg, as: 'legs' }, 'legIndex', 'ASC'] 
        ]
      });
      console.log(`Found ${routes.length} route batches.`);
      const processedRoutes = routes.map(processRouteWithLegs).filter(r => r !== null);
      res.json(processedRoutes);
    } catch (error) {
      console.error('Error fetching routes:', error);
      res.status(500).json({ error: 'Failed to fetch routes', details: error.message });
    }
  },

  create: async (req, res) => {
    let transaction;
    try {
      // routeData from frontend contains the array of leg objects
      const { routeData, ...routeInfo } = req.body;
      console.log('Received routeData for creation:', JSON.stringify(routeData ? routeData.slice(0, 1) : 'Empty', null, 2)); // Log first leg

      if (!Array.isArray(routeData) || routeData.length === 0) {
         console.log('Route creation attempt with empty or invalid routeData array.');
         // Decide if this is an error or just means no legs to save
         // return res.status(400).json({ error: 'routeData array is required and cannot be empty' });
      }

      const uuid = uuidv4();

      // Calculate totals based on the incoming routeData (array of legs)
      const totalEmployees = (routeData || []).reduce((total, leg) => total + (leg.employees?.length || 0), 0);
      const totalRoutes = (routeData || []).length; // Number of legs
      const averageOccupancy = totalEmployees > 0 && totalRoutes > 0 ? totalEmployees / totalRoutes : 0;

      transaction = await Route.sequelize.transaction();

      // Create the main route batch record
      const newRoute = await Route.create({
        ...routeInfo, // date, shift, tripType, profileId, facilityId etc.
        uuid,
        totalEmployees,
        totalRoutes,
        averageOccupancy,
        profile: routeInfo.profile || null,
        facility: routeInfo.facility || null,
        employeeData: routeInfo.employeeData || null, // Overall employee list if sent
        routeDetails: routeInfo.routeDetails || null
      }, { transaction });

      console.log('Route batch record created, UUID:', uuid);

      // Process route legs data
      let totalDistance = 0;
      let totalDuration = 0;
      const routeLegsData = [];

      if (routeData && routeData.length > 0) {
        // Process all legs to collect data
        for (let index = 0; index < routeData.length; index++) {
          const leg = routeData[index];
          
          // Generate polyline for each leg if needed
          if (!leg.encodedPolyline && leg.coordinates) {
            try {
              // Convert coordinates to polyline format
              const coords = leg.coordinates.map(coord => [coord[1], coord[0]]).join(';');
              const response = await fetch(`http://localhost:5000/route/v1/driving/${coords}?geometries=polyline`);
              if (response.ok) {
                const data = await response.json();
                leg.encodedPolyline = data.routes[0].geometry;
                leg.distance = data.routes[0].distance;
                leg.duration = data.routes[0].duration;
              }
            } catch (error) {
              console.error(`Failed to encode polyline for leg ${index}:`, error);
            }
          }

          // Sum up distance and duration
          totalDistance += leg.distance || 0;
          totalDuration += leg.duration || 0;

          // Store leg data
          routeLegsData.push({
            legIndex: index,
            encodedPolyline: leg.encodedPolyline || null,
            distance: leg.distance || null,
            duration: leg.duration || null,
            employees: leg.employees || [],
          });
        }

        // Create individual RouteLeg records for each leg
        for (const legData of routeLegsData) {
          await RouteLeg.create({
            routeUuid: uuid,
            legIndex: legData.legIndex,
            encodedPolyline: legData.encodedPolyline,
            distance: legData.distance,
            duration: legData.duration,
            employees: legData.employees
          }, { transaction });
        }

        // Update the main route record with consolidated data
        await newRoute.update({
          routeDetails: {
            ...routeInfo.routeDetails,
            totalDistance,
            totalDuration
          }
        }, { transaction });

        console.log(`Consolidated ${routeData.length} route legs for route ${uuid}.`);
      } else {
        console.log(`No legs provided in routeData for route ${uuid}.`);
      }
      await transaction.commit();
      console.log('Transaction committed for route:', uuid);

      // Fetch the complete route with legs to return
      const finalRouteWithLegs = await Route.findOne({
          where: { uuid: uuid },
          include: [{ model: RouteLeg, as: 'legs' }]
      });
      const processedFinalRoute = processRouteWithLegs(finalRouteWithLegs);

      res.status(201).json(processedFinalRoute);

    } catch (error) {
      if (transaction && !transaction.finished) {
          try { await transaction.rollback(); console.log('Transaction rolled back due to error.'); } 
          catch (rollbackError) { console.error('Error rolling back transaction:', rollbackError); }
      }
      console.error('Error creating route:', error);
      const status = error.name === 'SequelizeValidationError' ? 400 : 500;
      res.status(status).json({ error: `Failed to create route: ${error.name}`, details: error.message });
    }
  },

  // Modify getByDateAndShift, getByProfile, getById similarly to use RouteLeg
  getByDateAndShift: async (req, res) => {
    try {
      const { date, shift } = req.query;
      if (!date || !shift) return res.status(400).json({ error: 'Date and shift required' });
      
      const routes = await Route.findAll({
        where: { date, shift },
        include: [{ model: RouteLeg, as: 'legs' }],
        order: [['createdAt', 'DESC']]
      });
      const processedRoutes = routes.map(processRouteWithLegs).filter(r => r !== null);
      res.json(processedRoutes);
    } catch (error) {
      console.error('Error fetching routes by date/shift:', error);
      res.status(500).json({ error: 'Failed to fetch routes' });
    }
  },

  getByProfile: async (req, res) => {
    try {
      const { profileId } = req.params;
      if (!profileId) return res.status(400).json({ error: 'Profile ID required' });
      const profileIdInt = parseInt(profileId, 10);
      if (isNaN(profileIdInt)) return res.status(400).json({ error: 'Invalid Profile ID' });

      const routes = await Route.findAll({
        where: { ProfileId: profileIdInt },
        include: [{ model: RouteLeg, as: 'legs' }],
        order: [['createdAt', 'DESC']]
      });
      const processedRoutes = routes.map(processRouteWithLegs).filter(r => r !== null);
      res.json(processedRoutes);
    } catch (error) {
      console.error('Error fetching routes by profile:', error);
      res.status(500).json({ error: 'Failed to fetch routes' });
    }
  },

  getById: async (req, res) => {
    try {
      const identifier = req.params.id;
      if (!identifier) return res.status(400).json({ error: 'Route ID/UUID required' });

      const queryOptions = {
        include: [{ model: RouteLeg, as: 'legs' }],
        where: {}
      };

      if (identifier.includes('-')) {
        queryOptions.where = { uuid: identifier };
      } else {
        const numericId = parseInt(identifier, 10);
        if (isNaN(numericId)) return res.status(400).json({ error: 'Invalid route identifier' });
        queryOptions.where = { id: numericId };
      }

      const route = await Route.findOne(queryOptions);

      if (!route) {
        return res.status(404).json({ error: 'Route not found' });
      }

      console.log('Route found, processing with legs...');
      const processedRoute = processRouteWithLegs(route);

      if (!processedRoute) {
         console.error('Error processing found route for identifier:', identifier);
         return res.status(500).json({ error: 'Error processing route data' });
      }

      res.json(processedRoute);
    } catch (error) {
      console.error(`Error fetching route ${req.params.id}:`, error);
      res.status(500).json({ error: 'Failed to fetch route', details: error.message });
    }
  }
};

module.exports = routeController;
