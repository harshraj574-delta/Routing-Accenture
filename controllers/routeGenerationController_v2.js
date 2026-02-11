/**
 * Route Generation Controller v2 (New/Enhanced)
 * Uses the improved routing algorithm with angular sector clustering,
 * singleton aggregation, and linear direction optimization.
 */

const { v4: uuidv4 } = require('uuid');
const routeGenerationService = require('../services/routeGenerationService');
const { routeRequestSchema, recalculateRouteRequestSchema, etaRequestSchema } = require('../validators/routeGenerationValidator');

const routeGenerationControllerV2 = {
    getEta: async (req, res) => {
        try {
            const parseResult = etaRequestSchema.safeParse(req.body);
            if (!parseResult.success) {
                const errorMessages = parseResult.error.errors.map(e => e.message);
                return res.status(400).json({
                    errorCode: "400",
                    error: errorMessages.join('; ')
                });
            }
            const { routes, shiftTime, city } = parseResult.data;

            const etaResults = await Promise.all(routes.map(async (route) => {
                const { routeId, currentGeocodes, destinationGeocodes } = route;
                const duration = await routeGenerationService.calculateEta(
                    currentGeocodes,
                    destinationGeocodes,
                    shiftTime,
                    city
                );
                return { routeId, duration };
            }));

            return res.status(200).json({ etas: etaResults });
        } catch (error) {
            console.error('[v2] Error calculating ETA:', error);
            res.status(500).json({ error: 'Failed to calculate ETA', details: error.message });
        }
    },

    generateRoutes: async (req, res) => {
        let transaction;
        try {
            // Zod validation
            const parseResult = routeRequestSchema.safeParse(req.body);
            if (!parseResult.success) {
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

            // Shift time validation
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

            console.log(`[v2] Generating routes for ${employees.length} employees, date: ${date}, shift: ${shiftTime}`);
            console.log('[v2] Using enhanced routing algorithm with angular clustering and linear direction optimization');

            const uuid = uuidv4();

            if (zones && Array.isArray(zones) && zones.length > 0) {
                console.log(`[v2] Using ${zones.length} zones provided in request`);
            } else {
                console.log('[v2] No zones provided in request, will use zones from backend data file');
            }

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

            const routeResponse = await routeGenerationService.generateRoutes(routeGenerationData);

            // Add version info to response
            routeResponse.apiVersion = 'v2';

            return res.status(200).json(routeResponse);

        } catch (error) {
            if (transaction && !transaction.finished) {
                try {
                    await transaction.rollback();
                    console.log('[v2] Transaction rolled back due to error.');
                } catch (rollbackError) {
                    console.error('[v2] Error rolling back transaction:', rollbackError);
                }
            }
            console.error('[v2] Error generating routes:', error);
            res.status(500).json({ error: 'Failed to generate routes', details: error.message });
        }
    },

    recalculateRoute: async (req, res) => {
        try {
            const parseResult = recalculateRouteRequestSchema.safeParse(req.body);
            if (!parseResult.success) {
                const errorMessages = parseResult.error.errors.map(e => e.message);
                return res.status(400).json({
                    errorCode: "400",
                    error: errorMessages.join('; ')
                });
            }
            const { routes, facility, shiftTime, pickupTimePerEmployee, reportingTime, city, tripType } = parseResult.data;
            const isDropoff = tripType.toLowerCase() === "dropoff";

            const recalculatedRoutes = [];
            for (const route of routes) {
                const { employees, routeId } = route;
                const employeeCoordinates = employees.map(emp => [emp.geoY, emp.geoX]);
                const facilityCoordinates = [facility.geoY, facility.geoX];
                const routeCoordinates = isDropoff ? [facilityCoordinates, ...employeeCoordinates] : [...employeeCoordinates, facilityCoordinates];

                const routeDetails = await routeGenerationService.calculateRouteDetails(
                    routeCoordinates,
                    employees,
                    pickupTimePerEmployee,
                    tripType,
                    city,
                    shiftTime
                );

                const routeForEta = {
                    ...route,
                    tripType,
                    employees: routeDetails.employees,
                    routeDetails: routeDetails
                };

                routeGenerationService.calculatePickupTimes(
                    routeForEta,
                    shiftTime,
                    pickupTimePerEmployee,
                    reportingTime
                );

                const farthestEmployeeDistance = await routeGenerationService.calculateFarthestEmployeeDistance(
                    routeForEta,
                    facility,
                    city,
                    isDropoff
                );

                recalculatedRoutes.push({
                    routeId,
                    tripType,
                    employees: routeForEta.employees,
                    totalDistance: routeDetails.totalDistance,
                    totalDuration: routeDetails.totalDuration,
                    encodedPolyline: routeDetails.encodedPolyline,
                    farthestEmployeeDistance,
                });
            }

            return res.status(200).json({ recalculatedRoutes, apiVersion: 'v2' });
        } catch (error) {
            console.error('[v2] Error recalculating route:', error);
            res.status(500).json({ error: 'Failed to recalculate route', details: error.message });
        }
    },

    /**
     * Reoptimize route - uses VRP solver to find optimal stop order, then calculates ETA/polyline
     * Accepts same input as /recalculate but reorders employees before calculating route details
     */
    reoptimizeRoute: async (req, res) => {
        try {
            const parseResult = recalculateRouteRequestSchema.safeParse(req.body);
            if (!parseResult.success) {
                const errorMessages = parseResult.error.errors.map(e => e.message);
                return res.status(400).json({
                    errorCode: "400",
                    error: errorMessages.join('; ')
                });
            }
            const { routes, facility, shiftTime, pickupTimePerEmployee, reportingTime, city, tripType } = parseResult.data;
            const isDropoff = tripType.toLowerCase() === "dropoff";

            const reoptimizedRoutes = [];
            for (const route of routes) {
                const { employees, routeId, vehicleCapacity } = route;

                // Use VRP solver to optimize employee order
                const reoptResult = await routeGenerationService.reoptimizeRouteWithVRP({
                    employees,
                    facility,
                    tripType,
                    vehicleCapacity: vehicleCapacity || employees.length,
                    pickupTimePerEmployee,
                    city,
                    shiftTime
                });

                if (!reoptResult.reOptimized || reoptResult.error) {
                    console.warn(`[v2] Reoptimize failed for route ${routeId}: ${reoptResult.error || 'Unknown error'}, falling back to original order`);
                    // Fallback to regular recalculate
                    const employeeCoordinates = employees.map(emp => [emp.geoY, emp.geoX]);
                    const facilityCoordinates = [facility.geoY, facility.geoX];
                    const routeCoordinates = isDropoff ? [facilityCoordinates, ...employeeCoordinates] : [...employeeCoordinates, facilityCoordinates];

                    const routeDetails = await routeGenerationService.calculateRouteDetails(
                        routeCoordinates, employees, pickupTimePerEmployee, tripType, city, shiftTime
                    );

                    const routeForEta = { ...route, tripType, employees: routeDetails.employees, routeDetails };
                    routeGenerationService.calculatePickupTimes(routeForEta, shiftTime, pickupTimePerEmployee, reportingTime);

                    const farthestEmployeeDistance = await routeGenerationService.calculateFarthestEmployeeDistance(
                        routeForEta, facility, city, isDropoff
                    );

                    reoptimizedRoutes.push({
                        routeId, tripType,
                        employees: routeForEta.employees,
                        totalDistance: routeDetails.totalDistance,
                        totalDuration: routeDetails.totalDuration,
                        encodedPolyline: routeDetails.encodedPolyline,
                        farthestEmployeeDistance,
                        reoptimized: false
                    });
                    continue;
                }

                const routeForEta = {
                    ...route,
                    tripType,
                    employees: reoptResult.employees,
                    routeDetails: reoptResult.routeDetails
                };

                routeGenerationService.calculatePickupTimes(routeForEta, shiftTime, pickupTimePerEmployee, reportingTime);

                const farthestEmployeeDistance = await routeGenerationService.calculateFarthestEmployeeDistance(
                    routeForEta, facility, city, isDropoff
                );

                reoptimizedRoutes.push({
                    routeId, tripType,
                    employees: routeForEta.employees,
                    totalDistance: reoptResult.routeDetails.totalDistance,
                    totalDuration: reoptResult.routeDetails.totalDuration,
                    encodedPolyline: reoptResult.routeDetails.encodedPolyline,
                    farthestEmployeeDistance,
                    reoptimized: true
                });
            }

            return res.status(200).json({ recalculatedRoutes: reoptimizedRoutes, apiVersion: 'v2' });
        } catch (error) {
            console.error('[v2] Error reoptimizing route:', error);
            res.status(500).json({ error: 'Failed to reoptimize route', details: error.message });
        }
    }
};

module.exports = routeGenerationControllerV2;
