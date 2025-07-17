const { v4: uuidv4 } = require('uuid');
const routeGenerationService = require('../services/routeGenerationService');
const { routeRequestSchema } = require('../validators/routeGenerationValidator');


const routeGenerationController = {
  generateRoutes: async (req, res) => {
    let transaction;
    try {
      // Zod validation
      const parseResult = routeRequestSchema.safeParse(req.body);
      if (!parseResult.success) {
        // Collect all error messages, including per-employee
        const errorMessages = parseResult.error.errors.map(e => e.message);
        return res.status(200).json({
          errorCode: "400",
          error: errorMessages.join('; ')
        });
      }
      const {
        employees, facility, shiftTime, date, profile, saveToDatabase = false,
        pickupTimePerEmployee, reportingTime, tripType, guard = false, zones
      } = parseResult.data;

      // Normalize tripType
      let normalizedTripType = tripType;
      if (typeof normalizedTripType === 'string') {
        if (normalizedTripType.toUpperCase() === 'P') normalizedTripType = 'PICKUP';
        else if (normalizedTripType.toUpperCase() === 'D') normalizedTripType = 'DROPOFF';
        else normalizedTripType = normalizedTripType.toUpperCase();
      }

      // --- Shift time validation: only accept HHMM format ---
      let hours, minutes;
      const timeStr = shiftTime.toString().padStart(4, '0');
      hours = parseInt(timeStr.substring(0, 2), 10);
      minutes = parseInt(timeStr.substring(2, 4), 10);
      if (
        isNaN(hours) || isNaN(minutes) ||
        hours < 0 || hours > 23 ||
        minutes < 0 || minutes > 59
      ) {
        return res.status(200).json({
          errorCode: "400",
          error: 'shiftTime is invalid. It must be in HHMM (0000-2359) format.'
        });
      }

      console.log(`Generating routes for ${employees.length} employees, date: ${date}, shift: ${shiftTime}`);

      // Generate a UUID for this route batch
      const uuid = uuidv4();

      if (zones && Array.isArray(zones) && zones.length > 0) {
        console.log(`Using ${zones.length} zones provided in request`);
      } else {
        console.log('No zones provided in request, will use zones from backend data file');
      }

      // Prepare data for route generation service
      const routeGenerationData = {
        uuid,
        employees,
        facility,
        shiftTime,
        date,
        profile,
        zones,
        tripType: normalizedTripType,
        pickupTimePerEmployee,
        reportingTime,
        guard
      };

      // Call the route generation service to generate routes
      const routeResponse = await routeGenerationService.generateRoutes(routeGenerationData);

      // If not saving to database, just return the generated route data
      return res.status(200).json(routeResponse);

    } catch (error) {
      if (transaction && !transaction.finished) {
        try {
          await transaction.rollback();
          console.log('Transaction rolled back due to error.');
        } catch (rollbackError) {
          console.error('Error rolling back transaction:', rollbackError);
        }
      }
      console.error('Error generating routes:', error);
      res.status(500).json({ error: 'Failed to generate routes', details: error.message });
    }
  }
};

module.exports = routeGenerationController;
