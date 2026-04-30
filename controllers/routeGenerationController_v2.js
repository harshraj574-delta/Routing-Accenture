/**
 * Route Generation Controller v2 (New/Enhanced)
 * Uses the improved routing algorithm with angular sector clustering,
 * singleton aggregation, and linear direction optimization.
 */

const { v4: uuidv4 } = require('uuid');
const routeGenerationService = require('../services/routeGenerationService');
const { routeRequestSchema, recalculateRouteRequestSchema, etaRequestSchema } = require('../validators/routeGenerationValidator');
const JobStore = require('../db/jobStore');

// Works with native fetch (Node 18+) or falls back to node-fetch (v3, ESM)
const httpFetch = typeof globalThis.fetch === 'function'
    ? (...args) => globalThis.fetch(...args)
    : (...args) => import('node-fetch').then(({ default: f }) => f(...args));

async function callMainBackend(baseUrl, path, body) {
    const url = `${baseUrl.replace(/\/$/, '')}${path}`;
    const res = await httpFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`${path} returned ${res.status}: ${text.substring(0, 200)}`);
    }
    const data = await res.json();
    return typeof data === 'string' ? JSON.parse(data) : data;
}

async function callWithRetry(baseUrl, path, body, maxRetries = 3) {
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await callMainBackend(baseUrl, path, body);
        } catch (err) {
            lastError = err;
            if (attempt < maxRetries) {
                const delayMs = attempt * 5000; // 5s, 10s, 15s
                console.warn(`[v2 async] ${path} attempt ${attempt}/${maxRetries} failed: ${err.message}. Retrying in ${delayMs / 1000}s...`);
                await new Promise(r => setTimeout(r, delayMs));
            }
        }
    }
    throw lastError;
}

async function runGenerationJob(jobId, params) {
    const mainBackendUrl = process.env.MAIN_BACKEND_URL || params.mainBackendUrl || '';

    // Heartbeat: touch updatedAt every 30 s so the job doesn't appear stale
    // while the routing engine is doing heavy computation (can take several minutes).
    const heartbeat = setInterval(() => {
        JobStore.updateJob(jobId, {}).catch(() => {});
    }, 30 * 1000);

    try {
        await JobStore.updateJob(jobId, { status: 'running', progressMessage: 'Fetching route input data...', progressPercent: 10 });

        const routeInputRaw = await callMainBackend(mainBackendUrl, '/GetRouteInputJson', {
            facilityid: params.facilityid,
            sDate: params.sDate,
            triptype: params.triptype,
            shifttime: params.shifttime,
            locationID: '',
            updatedBy: params.updatedBy
        });

        const parseResult = routeRequestSchema.safeParse(routeInputRaw);
        if (!parseResult.success) {
            const msgs = parseResult.error.errors.map(e => e.message).join('; ');
            throw new Error(`Invalid route input from backend: ${msgs}`);
        }

        let { employees, facility, shiftTime, date, profile, zones, tripType, pickupTimePerEmployee, reportingTime, guard = false } = parseResult.data;

        if (typeof tripType === 'string') {
            if (tripType.toUpperCase() === 'P') tripType = 'PICKUP';
            else if (tripType.toUpperCase() === 'D') tripType = 'DROPOFF';
            else tripType = tripType.toUpperCase();
        }

        await JobStore.updateJob(jobId, { progressMessage: `Generating routes for ${employees.length} employees...`, progressPercent: 25 });
        console.log(`[v2 async] Job ${jobId}: generating routes for ${employees.length} employees`);

        const routeResponse = await routeGenerationService.generateRoutes({
            uuid: jobId,
            employees,
            facility,
            shiftTime,
            date,
            profile,
            zones,
            tripType,
            pickupTimePerEmployee,
            reportingTime,
            guard
        });

        routeResponse.apiVersion = 'v2';

        await JobStore.updateJob(jobId, { progressMessage: 'Saving generated routes...', progressPercent: 85 });

        try {
            await callWithRetry(mainBackendUrl, '/save_routesMapBasedNew', {
                facilityid: params.facilityid,
                sDate: params.sDate,
                triptype: params.triptype,
                shifttime: params.shifttime,
                jsonstring: JSON.stringify(routeResponse),
                updatedBy: 0,
                IsNewAdded: 0
            });
        } catch (saveErr) {
            // Routes were generated successfully but the save failed after all retries.
            // Throw a specific message so the admin knows generation was not the problem.
            throw new Error(
                `Routes were generated successfully but could not be saved to the server. ` +
                `Please try again shortly. (${saveErr.message})`
            );
        }

        await JobStore.updateJob(jobId, { status: 'completed', progressMessage: 'Routes generated successfully!', progressPercent: 100 });
        console.log(`[v2 async] Job ${jobId}: completed`);

    } catch (err) {
        console.error(`[v2 async] Job ${jobId} failed:`, err.message);
        await JobStore.updateJob(jobId, {
            status: 'failed',
            progressMessage: 'Route generation failed',
            errorMessage: err.message
        }).catch(() => {});
    } finally {
        clearInterval(heartbeat);
    }
}

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
    generateRoutesAsync: async (req, res) => {
        try {
            const { facilityid, sDate, triptype, shifttime, updatedBy, mainBackendUrl } = req.body;

            if (!facilityid || !sDate || !triptype || !shifttime) {
                return res.status(400).json({ error: 'facilityid, sDate, triptype, shifttime are required' });
            }
            if (!process.env.MAIN_BACKEND_URL && !mainBackendUrl) {
                return res.status(500).json({ error: 'MAIN_BACKEND_URL is not configured on the server' });
            }

            const newJobId = uuidv4();
            const params = { facilityid, sDate, triptype, shifttime, updatedBy, mainBackendUrl };

            // Atomic check-or-insert: if a live job already exists for this shift, return it
            const { jobId, created } = await JobStore.createOrGetJob(newJobId, params);

            if (!created) {
                console.log(`[v2 async] Returning existing job ${jobId} for facility=${facilityid} date=${sDate} triptype=${triptype}`);
                return res.status(202).json({ jobId, status: 'already_running', message: 'Route generation already in progress for this shift' });
            }

            // Fire-and-forget — response returns immediately, job runs in background
            setImmediate(() => runGenerationJob(jobId, params));

            return res.status(202).json({ jobId, status: 'pending', message: 'Route generation started' });
        } catch (error) {
            console.error('[v2] Error starting async route generation:', error);
            return res.status(500).json({ error: 'Failed to start route generation', details: error.message });
        }
    },

    checkInProgress: async (req, res) => {
        try {
            const { facilityid, sDate, triptype } = req.query;
            if (!facilityid || !sDate || !triptype) {
                return res.status(400).json({ error: 'facilityid, sDate, triptype are required query params' });
            }

            const job = await JobStore.getInProgressJob(facilityid, sDate, triptype);

            if (!job) {
                return res.status(200).json({ inProgress: false });
            }

            return res.status(200).json({
                inProgress: true,
                jobId: job.jobId,
                status: job.status,
                progressMessage: job.progressMessage,
                progressPercent: job.progressPercent,
                startedBy: job.updatedBy,
                startedAt: job.createdAt
            });
        } catch (error) {
            console.error('[v2] Error checking in-progress job:', error);
            // Return false on error so the caller is never blocked from generating
            return res.status(200).json({ inProgress: false });
        }
    },

    getJobStatus: async (req, res) => {
        try {
            const { jobId } = req.params;
            if (!jobId) return res.status(400).json({ error: 'jobId is required' });

            const job = await JobStore.getJob(jobId);
            if (!job) return res.status(404).json({ error: 'Job not found' });

            return res.status(200).json({
                jobId: job.jobId,
                status: job.status,
                progressMessage: job.progressMessage,
                progressPercent: job.progressPercent,
                errorMessage: job.errorMessage || null,
                createdAt: job.createdAt,
                updatedAt: job.updatedAt
            });
        } catch (error) {
            console.error('[v2] Error fetching job status:', error);
            return res.status(500).json({ error: 'Failed to fetch job status', details: error.message });
        }
    },

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
