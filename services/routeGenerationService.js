const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const { spawn } = require("child_process"); // For calling Python

const TRAFFIC_BUFFER_PERCENTAGE = 0.4; // 40% buffer for traffic
const MAX_SWAP_DISTANCE_KM = 1.5; // or your business threshold

// Constants for processEmployeeBatch (your original heuristic)
const OSRM_PROBE_COUNT_HEURISTIC = 0; // Can be different from OR-Tools related OSRM_PROBE_COUNT
const OSRM_PROBE_TIMEOUT_HEURISTIC = 3000;
const OSRM_PROBE_TIMEOUT = 8000;

const fetchApi = (...args) => {
  return import("node-fetch").then(({ default: fetch }) => fetch(...args));
};

// const ZONES_DATA_FILE = path.join(__dirname, "../data/delhi_ncr_zones.json");
const ZONES_DATA_FILE = path.join(__dirname, "../data/bengaluru_zones.json");

// --- All your existing helper functions (isOsrmAvailable, decode/encodePolyline, etc.) ---
async function isOsrmAvailable() {
  try {
    const osrmUrl =
      "http://localhost:5000/route/v1/driving/77.1025,28.7041;77.1026,28.7042";
    const response = await fetchApi(osrmUrl, {
      method: "GET",
      timeout: 8000,
    });
    if (response.ok) {
      const data = await response.json();
      return data && data.code === "Ok";
    }
    return false;
  } catch (error) {
    console.error("Error checking OSRM availability:", error);
    return false;
  }
}

function decodePolyline(encoded) {
  let index = 0;
  const len = encoded.length;
  const decoded = [];
  let lat = 0;
  let lng = 0;
  while (index < len) {
    let b,
      shift = 0,
      result = 0;
    do {
      b = encoded.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    const dlat = result & 1 ? ~(result >> 1) : result >> 1;
    lat += dlat;
    shift = 0;
    result = 0;
    do {
      b = encoded.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    const dlng = result & 1 ? ~(result >> 1) : result >> 1;
    lng += dlng;
    decoded.push([lat * 1e-5, lng * 1e-5]);
  }
  return decoded;
}

function encodePolyline(coordinates) {
  let output = "";
  let prevLat = 0;
  let prevLng = 0;
  for (const [lat, lng] of coordinates) {
    const latInt = Math.round(lat * 1e5);
    const lngInt = Math.round(lng * 1e5);
    const dLat = latInt - prevLat;
    prevLat = latInt;
    output += encodeNumber(dLat);
    const dLng = lngInt - prevLng;
    prevLng = lngInt;
    output += encodeNumber(dLng);
  }
  return output;
}

function encodeNumber(num) {
  num = num < 0 ? ~(num << 1) : num << 1;
  let output = "";
  while (num >= 0x20) {
    output += String.fromCharCode((0x20 | (num & 0x1f)) + 63);
    num >>= 5;
  }
  output += String.fromCharCode(num + 63);
  return output;
}

function toRadians(degrees) {
  return degrees * (Math.PI / 180);
}

async function calculateDistance(point1, point2) {
  const [lat1, lng1] = point1;
  const [lat2, lng2] = point2;
  const R = 6371;
  const dLat = toRadians(lat2 - lat1);
  const dLng = toRadians(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLng / 2) *
      Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function haversineDistance([lat1, lon1], [lat2, lon2]) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371; // km
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function isPointInPolygon(point, polygon) {
  if (!point || !polygon || !Array.isArray(polygon)) return false;
  const [lat, lng] = point;
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const [lngI, latI] = polygon[i];
    const [lngJ, latJ] = polygon[j];
    const intersect =
      latI > lat !== latJ > lat &&
      lng < ((lngJ - lngI) * (lat - latI)) / (latJ - latI) + lngI;
    if (intersect) inside = !inside;
  }
  return inside;
}

function calculateAngle(point1, point2) {
  return Math.atan2(point2.lat - point1.lat, point2.lng - point1.lng);
}

function angleDifference(angle1, angle2) {
  let diff = Math.abs(angle1 - angle2);
  if (diff > Math.PI) {
    diff = 2 * Math.PI - diff;
  }
  return diff;
}

async function loadZonesData() {
  const data = await fs.promises.readFile(ZONES_DATA_FILE, "utf8");
  const zonesData = JSON.parse(data);
  return zonesData.features;
}

function assignEmployeesToZones(employees, zones) {
  const employeesByZone = {};
  const assignedEmployees = new Set();
  for (let i = 0; i < zones.length; i++) {
    const zone = zones[i];
    const zoneName = zone.properties?.Name || "Unknown Zone";
    const zonePolygon = zone.geometry.coordinates[0];
    const zoneEmployees = employees
      .filter((emp) => {
        if (!emp.geoX || !emp.geoY) return false;
        const isInZone = isPointInPolygon([emp.geoY, emp.geoX], zonePolygon);
        if (isInZone) assignedEmployees.add(emp.empCode);
        return isInZone;
      })
      .map((emp) => ({
        ...emp,
        zone: zoneName,
        location: { lat: emp.geoY, lng: emp.geoX },
      }));
    if (zoneEmployees.length > 0) employeesByZone[zoneName] = zoneEmployees;
  }
  const unassignedEmployees = employees.filter(
    (emp) => !assignedEmployees.has(emp.empCode) && emp.geoX && emp.geoY
  );
  if (unassignedEmployees.length > 0) {
    const defaultZoneName = "DEFAULT_ZONE";
    employeesByZone[defaultZoneName] = unassignedEmployees.map((emp) => ({
      ...emp,
      zone: defaultZoneName,
      location: { lat: emp.geoY, lng: emp.geoX },
    }));
  }
  return employeesByZone;
}

function findZoneGroups(zonePairingMatrix) {
  const visited = new Set();
  const groups = [];
  for (const zone in zonePairingMatrix) {
    if (visited.has(zone)) continue;
    const group = [];
    const queue = [zone];
    visited.add(zone);
    while (queue.length > 0) {
      const current = queue.shift();
      group.push(current);
      const neighbors = zonePairingMatrix[current] || [];
      for (const neighbor of neighbors) {
        if (!visited.has(neighbor)) {
          visited.add(neighbor);
          queue.push(neighbor);
        }
      }
    }
    if (group.length > 0) groups.push(group);
  }
  return groups;
}

function getZoneCapacity(zoneName, profile) {
  if (profile.LargeCapacityZones?.includes(zoneName)) return 12;
  if (profile.MediumCapacityZones?.includes(zoneName)) return 6;
  if (profile.SmallCapacityZones?.includes(zoneName)) return 4;
  return 6;
}

// Add this helper function somewhere accessible within routeGenerationService.js

/**
 * Checks if the given shift time and trip type fall within the night shift
 * requiring a guard for female employees.
 * @param {string|number} shiftTime - The shift time (e.g., "0900", "2330", or 900, 2330).
 * @param {string} tripType - "PICKUP" or "DROPOFF".
 * @param {object} profile - The profile object, potentially containing night shift timings.
 * @returns {boolean}
 */
// In routeGenerationService.js

/**
 * Checks if the given shift time and trip type fall within the night shift
 * requiring a guard for female employees.
 * @param {string|number} shiftTime - The shift time (e.g., "0900", "2330", or 900, 2330).
 * @param {string} tripType - "PICKUP" or "DROPOFF".
 * @param {object} profile - The profile object, potentially containing night shift timings.
 * @returns {boolean}
 */
function isNightShiftForGuard(shiftTime, tripType, profile) {
  if (!shiftTime || !tripType) {
    // console.warn("[isNightShiftForGuard] Missing shiftTime or tripType.");
    return false;
  }

  // Standardize shiftTime to a number (e.g., "2330" -> 2330, "0700" -> 700)
  const st = parseInt(shiftTime.toString(), 10);
  if (isNaN(st)) {
    // console.warn(`[isNightShiftForGuard] Invalid shiftTime format: ${shiftTime}`);
    return false;
  }

  let nightShiftConfig = profile?.nightShiftGuardTimings;
  if (!nightShiftConfig) {
    // Default timings if not in profile (example based on CDC from SL #5)
    // It's better if these always come from the profile for clarity and configurability.
    // console.warn("[isNightShiftForGuard] Night shift timings not found in profile, using defaults.");
    nightShiftConfig = {
      PICKUP: { start: 2000, end: 700 },   // 8:00 PM to 07:00 AM (inclusive of 07:00)
      DROPOFF: { start: 1900, end: 530 },  // 7:00 PM to 05:30 AM (inclusive of 05:30)
      // DDC_DROPOFF: { start: 1830, end: 600 } // Example for DDC
    };
  }

  // Construct key based on facility type if available in profile, else default
  const facilityTypePrefix = (profile?.facilityType || "CDC").toUpperCase(); // Default to CDC if not specified
  const typeConfigKeyWithFacility = `${facilityTypePrefix}_${tripType.toUpperCase()}`;
  let config = nightShiftConfig[typeConfigKeyWithFacility];

  if (!config) { // Fallback to generic tripType if facility-specific is not found
      config = nightShiftConfig[tripType.toUpperCase()];
  }

  if (!config) {
    // console.warn(`[isNightShiftForGuard] No night shift config for key: ${typeConfigKeyWithFacility} or ${tripType.toUpperCase()}`);
    return false;
  }

  const { start, end } = config; // These should be numbers like 2000, 700

 
  if (start > end) { 
    if (st >= start || st <= end) { 
      return true;
    }
  } else if (start < end) { 
    if (st >= start && st <= end) { 
      return true;
    }
  } else { 
    if (st === start) {
        return true;
    }
  }
  return false;
}

const isSpecialNeedsUser = (emp) => {
  if (!emp) return false; // Guard against undefined employee object
  return (emp.isMedical || false) || (emp.isPWD || false);
};



async function calculateRouteDetails(
  routeCoordinates,
  employees,
  pickupTimePerEmployee,
  tripType = "pickup"
) {
  try {
    if (!routeCoordinates?.length || !employees?.length) {
      throw new Error("Invalid input parameters for calculateRouteDetails");
    }
    const coordinatesString = routeCoordinates
      .map((c) => `${c[1]},${c[0]}`)
      .join(";");
    const url = `http://localhost:5000/trip/v1/driving/${coordinatesString}?source=first&destination=last&roundtrip=false&steps=true&geometries=polyline&overview=full`;
    const response = await fetchApi(url);
    if (!response.ok) throw new Error(`OSRM /trip error: ${response.status}`);
    const data = await response.json();
    if (data.code !== "Ok" || !data.trips?.[0])
      throw new Error("Invalid OSRM /trip response");

    const trip = data.trips[0];
    const waypoints = data.waypoints;
    let fullRoadPolyline = trip.geometry || "";
    if (!fullRoadPolyline && trip.legs) {
      let fullCoords = [];
      for (const leg of trip.legs) {
        for (const step of leg.steps) {
          const coords = decodePolyline(step.geometry);
          if (fullCoords.length > 0 && coords.length > 0) {
            const lastPt = fullCoords[fullCoords.length - 1];
            if (lastPt[0] === coords[0][0] && lastPt[1] === coords[0][1])
              fullCoords.pop();
          }
          fullCoords = fullCoords.concat(coords);
        }
      }
      fullRoadPolyline = encodePolyline(fullCoords);
    }

    let orderedEmployees;
    if (tripType.toLowerCase() === "pickup") {
      orderedEmployees = waypoints
        .slice(0, -1)
        .sort((a, b) => a.trips_index - b.trips_index)
        .map((wp, i) => ({ ...employees[wp.waypoint_index], order: i + 1 }));
    } else {
      orderedEmployees = waypoints
        .slice(1)
        .sort((a, b) => a.trips_index - b.trips_index)
        .map((wp, i) => ({
          ...employees[wp.waypoint_index - 1],
          order: i + 1,
        }));
    }
    return {
      employees: orderedEmployees,
      totalDistance: trip.distance,
      totalDuration: trip.duration * (1 + TRAFFIC_BUFFER_PERCENTAGE),
      encodedPolyline: fullRoadPolyline,
      legs: trip.legs || [],
      geometry: {
        type: "LineString",
        coordinates: decodePolyline(fullRoadPolyline).map((c) => [c[1], c[0]]),
      },
    };
  } catch (error) {
    console.error("calculateRouteDetails error:", error);
    return {
      employees: employees.map((e, i) => ({ ...e, order: i + 1 })),
      totalDistance: 0,
      totalDuration: 0,
      encodedPolyline: "",
      legs: [],
      geometry: null,
      error: error.message,
    };
  }
}

// Add this function in your routeGenerationService.js

async function reOptimizeSwappedRouteWithORTools(
  routeToReOptimize, // The route object after a successful swap
  facilityData,
  pickupTimePerEmployee
  // profileMaxDuration, // Already available in routeToReOptimize or facilityData.profile
) {
  const {
    employees: swappedEmployees,
    tripType,
    zone,
    vehicleCapacity, // Original capacity before guard reduction
    isMedicalRoute,
  } = routeToReOptimize;
  const profileMaxDuration = facilityData.profile?.maxDuration || 7200;

  if (!swappedEmployees || swappedEmployees.length === 0) {
    console.warn(
      `[RE-OPTIMIZE] Route for zone ${zone} has no employees after swap. Skipping re-optimization.`
    );
    return {
      reOptimized: false,
      employees: swappedEmployees,
      error: "No employees for re-optimization",
    };
  }

  console.log(
    `\n[RE-OPTIMIZE OR-TOOLS] Re-optimizing swapped route for zone: "${zone}" with ${swappedEmployees.length} employees.`
  );

  const facilityLocation = {
    lat: facilityData.geoY,
    lng: facilityData.geoX,
  };
  let pinnedEmployee;
  let otherEmployeesInRoute;
  let fixedNodeParam = {};

  // The `employees` in routeToReOptimize are already in the swapped order
  // from handleGuardRequirements and its OSRM /trip call.
  // The critical employee (now male) is at the start/end.
  if (tripType.toLowerCase() === "pickup") {
    pinnedEmployee = swappedEmployees[0];
    otherEmployeesInRoute = swappedEmployees.slice(1);
  } else {
    // Dropoff
    pinnedEmployee = swappedEmployees[swappedEmployees.length - 1];
    otherEmployeesInRoute = swappedEmployees.slice(
      0,
      swappedEmployees.length - 1
    );
  }

  if (!pinnedEmployee) {
    console.error(
      `[RE-OPTIMIZE] Could not identify pinned employee for zone ${zone}.`
    );
    return { reOptimized: false, employees: swappedEmployees };
  }

  // Prepare a new pointMap and matrices specifically for this re-optimization call
  // The order in this pointMap will define the indices sent to Python
  const employeesForThisOrRun = [pinnedEmployee, ...otherEmployeesInRoute];
  const pointMapForReSolve = [
    { empCode: "FACILITY", isFacility: true, ...facilityLocation },
    ...employeesForThisOrRun.map((emp) => ({ ...emp })), // Ensure fresh objects
  ];

  // The pinned employee will always be at index 1 in pointMapForReSolve (depot is 0)
  const pinnedNodeIndexInMatrix = 1;
  const otherCustomerIndicesInMatrix = otherEmployeesInRoute.map(
    (_, i) => i + 2
  ); // Start from index 2

  if (tripType.toLowerCase() === "pickup") {
    fixedNodeParam = {
      fixed_start_node_index_in_matrix: pinnedNodeIndexInMatrix,
    };
  } else {
    fixedNodeParam = {
      fixed_end_node_index_in_matrix: pinnedNodeIndexInMatrix,
      other_customer_node_indices_in_matrix: otherCustomerIndicesInMatrix,
    };
  }

  try {
    const matrixData = await generateDistanceDurationMatrix(
      employeesForThisOrRun, // Only the employees in this specific route
      facilityLocation
    );
    const { distanceMatrix, durationMatrix } = matrixData;

    if (
      !distanceMatrix ||
      distanceMatrix.length === 0 ||
      (distanceMatrix.length > 0 && distanceMatrix[0].length === 0)
    ) {
      console.warn(
        `[RE-OPTIMIZE] Empty/invalid distance matrix for re-optimizing zone "${zone}".`
      );
      return { reOptimized: false, employees: swappedEmployees };
    }
    if (pointMapForReSolve.length !== distanceMatrix.length) {
      console.error(
        `[RE-OPTIMIZE] Mismatch pointMap length and matrix dimensions for re-optimizing zone "${zone}"!`
      );
      return { reOptimized: false, employees: swappedEmployees };
    }

    const demands = [0, ...employeesForThisOrRun.map(() => 1)]; // Depot + N employees
    const serviceTimes = [
      0,
      ...employeesForThisOrRun.map(() => pickupTimePerEmployee),
    ];

    const orToolsInput = {
      distance_matrix: distanceMatrix,
      duration_matrix: durationMatrix,
      num_vehicles: 1, // We are optimizing a single, existing route
      vehicle_capacities: [vehicleCapacity], // Use original capacity
      demands: demands,
      depot_index: 0,
      max_route_duration: profileMaxDuration,
      service_times: serviceTimes,
      allow_dropping_visits: false, // Do not allow dropping in re-optimization
      // drop_visit_penalty: facilityData.profile?.dropPenalty || 36000,
      facility_coords: [facilityLocation.lat, facilityLocation.lng],
      trip_type: tripType.toUpperCase(),
      direction_penalty_weight:
        facilityData.profile?.directionPenaltyWeight || 0.5, // Maybe lower for re-opt
      ...fixedNodeParam, // Add the new fixed node parameters
    };

    const pythonExecutable = "python";
    const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py");
    if (!fs.existsSync(scriptPath)) {
      throw new Error(`Solver script not found: ${scriptPath}`);
    }

    const pythonProcess = spawn(pythonExecutable, [scriptPath]);
    let scriptOutput = "";
    let scriptError = "";

    pythonProcess.stdin.write(JSON.stringify(orToolsInput));
    pythonProcess.stdin.end();
    pythonProcess.stdout.on("data", (data) => {
      scriptOutput += data.toString();
    });
    pythonProcess.stderr.on("data", (data) => {
      const errData = data.toString();
      console.error(`[RE-OPTIMIZE Python stderr FOR ZONE ${zone}]: ${errData}`);
      scriptError += errData;
    });

    return new Promise((resolve, reject) => {
      pythonProcess.on("close", (code) => {
        // ... (Standard JSON parsing and error handling for scriptOutput)
        // (Similar to what's in solveZoneWithORTools)
        let solution = null;
        let parsedSuccessfully = false;
        // (Copy parsing logic from solveZoneWithORTools here)
        // For brevity, assuming parsing logic is copied and works:
        try {
          // Simplified parsing for example
          const lines = scriptOutput.trim().split("\n");
          const lastLine = lines[lines.length - 1].trim();
          if (lastLine.startsWith("{") && lastLine.endsWith("}")) {
            solution = JSON.parse(lastLine);
            parsedSuccessfully = true;
          } else {
            throw new Error("Last line not valid JSON for re-opt.");
          }
        } catch (e) {
          console.error(
            `[RE-OPTIMIZE] Error parsing Python stdout for re-optimizing zone "${zone}":`,
            e,
            "\nRaw stdout:\n",
            scriptOutput
          );
          return resolve({
            reOptimized: false,
            employees: swappedEmployees,
            error: "Parse error",
          });
        }

        if (code !== 0) {
          console.error(
            `[RE-OPTIMIZE] Python script (zone "${zone}") exit ${code}. Stderr: ${scriptError}`
          );
          return resolve({
            reOptimized: false,
            employees: swappedEmployees,
            error: `Python exit ${code}`,
          });
        }
        if (!parsedSuccessfully || !solution || solution.error) {
          console.error(
            `[RE-OPTIMIZE] Failed to get valid OR-Tools solution for zone "${zone}". Solution: ${JSON.stringify(
              solution
            )}`
          );
          return resolve({
            reOptimized: false,
            employees: swappedEmployees,
            error: solution?.error || "No solution",
          });
        }
        if (
          solution.dropped_node_indices &&
          solution.dropped_node_indices.length > 0
        ) {
          console.warn(
            `[RE-OPTIMIZE] OR-Tools dropped nodes during re-optimization for zone ${zone}, which should not happen. Using original swapped order.`
          );
          return resolve({
            reOptimized: false,
            employees: swappedEmployees,
            error: "Nodes dropped in re-opt",
          });
        }

        if (
          solution.routes &&
          Array.isArray(solution.routes) &&
          solution.routes.length > 0 &&
          solution.routes[0].length > 0
        ) {
          const routeNodeIndices = solution.routes[0]; // Expecting one route
          const reOptimizedEmployeeList = routeNodeIndices
            .map((nodeIndex) => {
              if (nodeIndex === 0) return null; // Depot
              // nodeIndex is 1-based for customers from Python if depot is 0
              // It maps to pointMapForReSolve[nodeIndex]
              if (nodeIndex >= pointMapForReSolve.length) {
                console.error(
                  `[RE-OPTIMIZE] Route nodeIndex ${nodeIndex} out of bounds for pointMap length ${pointMapForReSolve.length}`
                );
                return null;
              }
              return pointMapForReSolve[nodeIndex]; // empCode, geoX, geoY etc.
            })
            .filter((emp) => emp != null && !emp.isFacility);

          console.log(
            `[RE-OPTIMIZE] Successfully re-optimized route for zone ${zone}. New order length: ${reOptimizedEmployeeList.length}`
          );
          resolve({
            reOptimized: true,
            employees: reOptimizedEmployeeList,
          });
        } else {
          console.warn(
            `[RE-OPTIMIZE] OR-Tools returned no valid route for re-optimization in zone ${zone}. Using original swapped order.`
          );
          resolve({
            reOptimized: false,
            employees: swappedEmployees,
            error: "No route from re-opt",
          });
        }
      });
      pythonProcess.on("error", (err) => {
        console.error(
          `[RE-OPTIMIZE] Failed to start Python subprocess for zone "${zone}".`,
          err
        );
        resolve({
          reOptimized: false,
          employees: swappedEmployees,
          error: "Python spawn error",
        });
      });
    });
  } catch (error) {
    console.error(
      `[RE-OPTIMIZE] Critical error in reOptimizeSwappedRouteWithORTools for zone "${zone}":`,
      error
    );
    return {
      reOptimized: false,
      employees: swappedEmployees,
      error: error.message,
    };
  }
}

// --- YOUR ORIGINAL HEURISTIC BATCH PROCESSOR ---
// routeGenerationService.js

// ... (all other helper functions like isOsrmAvailable, calculateDistance, etc. remain the same)
// ... (reOptimizeSwappedRouteWithORTools, generateDistanceDurationMatrix, solveZoneWithORTools also remain the same for now)

// In routeGenerationService.js
// REPLACE your existing processEmployeeBatch with this one:
async function processEmployeeBatch(
  employees,
  maxCapacity, // Original physical capacity
  facility,
  tripType = "pickup",
  maxDuration, // Not directly used in this heuristic for capacity, but good to have
  pickupTimePerEmployee, // Not directly used in this heuristic for capacity
  guard = false,
) {
  const routes = [];
  const isDropoff = tripType.toLowerCase() === "dropoff";

  const validEmployees = employees.filter(
    (emp) =>
      emp.location &&
      typeof emp.location.lat === "number" &&
      typeof emp.location.lng === "number" &&
      !isNaN(emp.location.lat) &&
      !isNaN(emp.location.lng),
  );

  if (validEmployees.length === 0) {
    // console.warn("[processEmployeeBatch] No valid employees found.");
    return { routes: [] };
  }

  // Helper to determine if an employee is a special needs user
  const isSpecialNeedsUser = (emp) => (emp.isMedical || false) || (emp.isPWD || false);

  let globalRemainingEmployees = [...validEmployees].map((emp) => ({
    ...emp,
    distToFacility: haversineDistance(
      [emp.location.lat, emp.location.lng],
      [facility.geoY, facility.geoX],
    ),
    isMedical: emp.isMedical || false,
    isPWD: emp.isPWD || false,
  }));

  globalRemainingEmployees.sort((a, b) =>
    isDropoff
      ? a.distToFacility - b.distToFacility
      : b.distToFacility - a.distToFacility,
  );

  const facilityLocation = { lat: facility.geoY, lng: facility.geoX };
  const MAX_NEXT_STOP_DISTANCE_KM = MAX_SWAP_DISTANCE_KM * 1.5;
  const SCORE_DIFFERENCE_TOLERANCE = 0.1;
  const DISTANCE_SCORE_SCALAR = 10;
  const PROGRESS_WEIGHT = 1.0;
  const DISTANCE_WEIGHT = 0.8;
  const ACCEPTABLE_PROGRESS_FACTOR_PICKUP = 2.5;
  const ACCEPTABLE_PROGRESS_FACTOR_DROPOFF = 0.95;
  const PROGRESS_PENALTY_SCALAR = 20;

  let heuristicRouteCounter = 0;

  mainLoop: while (globalRemainingEmployees.length > 0) {
    heuristicRouteCounter++;
    const originalPhysicalCapacity = maxCapacity;
    let routeIsCurrentlySpecialNeeds = false;
    let currentRouteMaxAllowedOccupancy = originalPhysicalCapacity;

    const firstEmployeeForThisRoute = globalRemainingEmployees.shift();
    if (!firstEmployeeForThisRoute) break;

    const currentAttemptRouteEmployees = [firstEmployeeForThisRoute];
    // Create a true copy of the remaining employees for this attempt's pool
    let tempRemainingEmployeesForThisAttempt = globalRemainingEmployees.map(e => ({...e}));


    if (isSpecialNeedsUser(firstEmployeeForThisRoute)) {
      routeIsCurrentlySpecialNeeds = true;
      currentRouteMaxAllowedOccupancy = 2;
      // console.log(`Route ${heuristicRouteCounter}: Started with special needs emp ${firstEmployeeForThisRoute.empCode}. Max cap 2.`);
    } else {
      // console.log(`Route ${heuristicRouteCounter}: Started with regular emp ${firstEmployeeForThisRoute.empCode}. Max cap ${originalPhysicalCapacity}.`);
    }

    // Inner loop to add more employees
    while (
      currentAttemptRouteEmployees.length < currentRouteMaxAllowedOccupancy &&
      tempRemainingEmployeesForThisAttempt.length > 0
    ) {
      const currentLastEmployeeInRoute = currentAttemptRouteEmployees[currentAttemptRouteEmployees.length - 1];
      const currentLoc = currentLastEmployeeInRoute.location;

      let candidateIndexToRemove = -1; // To remove from tempRemainingEmployeesForThisAttempt

      let scoredCandidates = tempRemainingEmployeesForThisAttempt
        .map((candidateEmp, candidateIdx) => { // Keep track of index for removal
          const candidateIsSpecial = isSpecialNeedsUser(candidateEmp);

          if (routeIsCurrentlySpecialNeeds) {
            if (!candidateIsSpecial) {
              // console.log(`  Route ${heuristicRouteCounter} (Special): Skip regular cand ${candidateEmp.empCode}.`);
              return null;
            }
          } else { // Route is currently REGULAR
            if (candidateIsSpecial) {
              // If first emp was regular, cannot add a special needs person to make a mixed special route.
              // The rule is "both employees should be either PWD/medical cab user" for a special route.
              if (currentAttemptRouteEmployees.length > 0 && !isSpecialNeedsUser(currentAttemptRouteEmployees[0])) {
                // console.log(`  Route ${heuristicRouteCounter} (Regular): Skip special cand ${candidateEmp.empCode} as first emp ${currentAttemptRouteEmployees[0].empCode} is regular.`);
                return null;
              }
            }
          }

          const distanceToLastHaversine = haversineDistance([currentLoc.lat, currentLoc.lng], [candidateEmp.location.lat, candidateEmp.location.lng]);
          if (distanceToLastHaversine > MAX_NEXT_STOP_DISTANCE_KM) return null;

          let progressScore = 0;
          if (isDropoff) {
            const d = candidateEmp.distToFacility - currentLastEmployeeInRoute.distToFacility;
            progressScore = d * PROGRESS_WEIGHT * (candidateEmp.distToFacility >= currentLastEmployeeInRoute.distToFacility * ACCEPTABLE_PROGRESS_FACTOR_DROPOFF ? 1 : PROGRESS_PENALTY_SCALAR);
          } else {
            const d = currentLastEmployeeInRoute.distToFacility - candidateEmp.distToFacility;
            progressScore = d * PROGRESS_WEIGHT * (candidateEmp.distToFacility < currentLastEmployeeInRoute.distToFacility * ACCEPTABLE_PROGRESS_FACTOR_PICKUP ? 1 : PROGRESS_PENALTY_SCALAR);
          }
          const distanceScoreVal = (1 / (1 + distanceToLastHaversine)) * DISTANCE_WEIGHT * DISTANCE_SCORE_SCALAR;
          return { emp: candidateEmp, score: progressScore + distanceScoreVal, distanceToLast: distanceToLastHaversine, originalIndex: candidateIdx };
        })
        .filter(item => item != null && item.score > -Infinity);

      scoredCandidates.sort((a, b) => {
        if (Math.abs(b.score - a.score) > SCORE_DIFFERENCE_TOLERANCE) return b.score - a.score;
        return a.distanceToLast - b.distanceToLast;
      });

      if (scoredCandidates.length === 0) break;

      let nextEmployeeToPickData = scoredCandidates[0];
      let nextEmployeeToPick = nextEmployeeToPickData?.emp;

      if (OSRM_PROBE_COUNT_HEURISTIC > 0 && scoredCandidates.length > 0) {
        // OSRM Probe Logic would go here, potentially re-assigning nextEmployeeToPickData
      }

      if (!nextEmployeeToPick) break;

      // Final check based on strict homogeneity for special needs routes
      if (routeIsCurrentlySpecialNeeds && !isSpecialNeedsUser(nextEmployeeToPick)) {
          // This should ideally be caught by the map filter, but as a safeguard.
          // console.log(`  Route ${heuristicRouteCounter} (Special): FINAL CHECK - Skip regular cand ${nextEmployeeToPick.empCode}.`);
          tempRemainingEmployeesForThisAttempt.splice(nextEmployeeToPickData.originalIndex, 1); // Remove from consideration
          continue; // Try next best candidate from scoredCandidates (if any)
      }
      if (!routeIsCurrentlySpecialNeeds && isSpecialNeedsUser(nextEmployeeToPick)) {
          // If current route is regular and we are about to add a special needs person,
          // all existing people in the route must also be special needs (which means the first one had to be).
          const allExistingAreSpecial = currentAttemptRouteEmployees.every(isSpecialNeedsUser);
          if (!allExistingAreSpecial) {
            // console.log(`  Route ${heuristicRouteCounter} (Regular): FINAL CHECK - Skip special cand ${nextEmployeeToPick.empCode} as existing are not all special.`);
            tempRemainingEmployeesForThisAttempt.splice(nextEmployeeToPickData.originalIndex, 1);
            continue; // Try next best candidate
          }
      }

      currentAttemptRouteEmployees.push(nextEmployeeToPick);
      if (isSpecialNeedsUser(nextEmployeeToPick) && !routeIsCurrentlySpecialNeeds) {
        routeIsCurrentlySpecialNeeds = true;
        currentRouteMaxAllowedOccupancy = 2;
        // console.log(`    Route ${heuristicRouteCounter}: Became special needs due to ${nextEmployeeToPick.empCode}. MaxAllowedOcc now 2.`);
      }
      // Remove the picked employee from tempRemainingEmployeesForThisAttempt using its originalIndex from the map
      // Need to re-find index if list was mutated by 'continue'
      const actualPickedIndexInTemp = tempRemainingEmployeesForThisAttempt.findIndex(e => e.empCode === nextEmployeeToPick.empCode);
      if (actualPickedIndexInTemp > -1) {
          tempRemainingEmployeesForThisAttempt.splice(actualPickedIndexInTemp, 1);
      }
    }

    // --- GUARD LOGIC and FINAL CAPACITY DETERMINATION ---
    let routeNeedsGuard = false;
    let finalRouteSpecialNeedsStatusAfterGuard = routeIsCurrentlySpecialNeeds;

    if (guard && currentAttemptRouteEmployees.length > 0) {
      const critIdx = isDropoff ? currentAttemptRouteEmployees.length - 1 : 0;
      if (
        currentAttemptRouteEmployees[critIdx].gender === "F" &&
        !currentAttemptRouteEmployees.some((e) => e.gender === "M")
      ) {
        routeNeedsGuard = true;
        let capacityTargetForGuardTrim = finalRouteSpecialNeedsStatusAfterGuard
          ? 2
          : Math.max(1, originalPhysicalCapacity - 1);

        while (currentAttemptRouteEmployees.length > capacityTargetForGuardTrim) {
          const removed = currentAttemptRouteEmployees.pop();
          if (removed) {
            tempRemainingEmployeesForThisAttempt.unshift(removed); // Put back to this attempt's remaining pool
            if (isSpecialNeedsUser(removed) && !currentAttemptRouteEmployees.some(isSpecialNeedsUser)) {
              finalRouteSpecialNeedsStatusAfterGuard = false;
              capacityTargetForGuardTrim = routeNeedsGuard
                ? Math.max(1, originalPhysicalCapacity - 1)
                : originalPhysicalCapacity;
            }
          } else { break; }
        }
      }
    }

    // --- FINAL DECISION TO PUSH ROUTE OR DISCARD ---
    let successfullyFormedRoute = false;
    if (currentAttemptRouteEmployees.length > 0) {
      let isValidForPush = true;
      // Final check for the strict "both/all must be special if route is special"
      if (finalRouteSpecialNeedsStatusAfterGuard) {
        if (!currentAttemptRouteEmployees.every(isSpecialNeedsUser)) {
          isValidForPush = false;
          // console.error(`Route ${heuristicRouteCounter} marked special but not all emps are special: ${currentAttemptRouteEmployees.map(e=>e.empCode + "(M:"+e.isMedical+",P:"+e.isPWD)+")"}. Discarding this attempt.`);
          // Employees (except firstEmployeeForThisRoute which was already shifted from global) go back.
          const othersToPutBack = currentAttemptRouteEmployees.slice(1);
          if (othersToPutBack.length > 0) {
            // Add to the front of tempRemaining, so they are considered soon by a *different* firstEmployee
            tempRemainingEmployeesForThisAttempt.unshift(...othersToPutBack);
          }
        }
      }

      if (isValidForPush) {
        const capacityToStoreOnRouteObject = finalRouteSpecialNeedsStatusAfterGuard
          ? 2
          : (routeNeedsGuard ? Math.max(1, originalPhysicalCapacity - 1) : originalPhysicalCapacity);

        if (currentAttemptRouteEmployees.length <= capacityToStoreOnRouteObject) {
          // console.log(`Route ${heuristicRouteCounter}: Pushing. Emps:${currentAttemptRouteEmployees.length}. Special:${finalRouteSpecialNeedsStatusAfterGuard}. StoredCap:${capacityToStoreOnRouteObject}. Guard:${routeNeedsGuard}`);
          routes.push({
            employees: [...currentAttemptRouteEmployees],
            routeNumber: heuristicRouteCounter,
            vehicleCapacity: capacityToStoreOnRouteObject,
            guardNeeded: routeNeedsGuard,
            uniqueKey: `${firstEmployeeForThisRoute.zone}_${heuristicRouteCounter}_${uuidv4()}`,
            zone: firstEmployeeForThisRoute.zone,
            tripType: isDropoff ? "dropoff" : "pickup",
            isSpecialNeedsRoute: finalRouteSpecialNeedsStatusAfterGuard, // Using the new flag
          });
          successfullyFormedRoute = true;
        } else {
            // This case means the route is oversized even for its determined type.
            // Should be rare if logic is correct. Put back employees other than first.
            // console.error(`Route ${heuristicRouteCounter} (Special:${finalRouteSpecialNeedsStatusAfterGuard}, Guard:${routeNeedsGuard}) OVERSIZED for final cap. Emps: ${currentAttemptRouteEmployees.length}, TargetCap: ${capacityToStoreOnRouteObject}. Discarding.`);
            const othersToPutBack = currentAttemptRouteEmployees.slice(1);
            if (othersToPutBack.length > 0) {
                tempRemainingEmployeesForThisAttempt.unshift(...othersToPutBack);
            }
        }
      }
    }

    // Update globalRemainingEmployees based on the outcome of this attempt
    if (successfullyFormedRoute) {
      // If successful, globalRemainingEmployees becomes what's left in tempRemainingEmployeesForThisAttempt
      // (which had employees removed for the current successful route).
      globalRemainingEmployees = tempRemainingEmployeesForThisAttempt;
    } else {
      // If failed, firstEmployeeForThisRoute is consumed.
      // The rest of the employees that were in tempRemainingEmployeesForThisAttempt
      // (including any unshifted during guard logic of this failed attempt,
      // and any from currentAttemptRouteEmployees that were put back)
      // should form the new globalRemainingEmployees.
      globalRemainingEmployees = tempRemainingEmployeesForThisAttempt;
    }
  } // End mainLoop
  return { routes };
}



// ... (rest of the file: solveZoneWithORTools, generateRoutes, etc.)

// --- OR-TOOLS RELATED FUNCTIONS ---
async function generateDistanceDurationMatrix(
  locationsForMatrix,
  facilityLocation
) {
  const allPointsCoords = [
    facilityLocation,
    ...locationsForMatrix.map((emp) => emp.location),
  ];
  if (allPointsCoords.length <= 1) {
    return { distanceMatrix: [[]], durationMatrix: [[]], pointMap: [] };
  }
  const coordinatesString = allPointsCoords
    .map((p) => `${p.lng},${p.lat}`)
    .join(";");
  const matrixTimeout = OSRM_PROBE_TIMEOUT + allPointsCoords.length * 200;
  const osrmTableUrl = `http://localhost:5000/table/v1/driving/${coordinatesString}?annotations=duration,distance`;
  // console.log(`[MatrixGen] OSRM Table URL for matrix (${allPointsCoords.length} points)`);

  try {
    const response = await fetchApi(osrmTableUrl, { timeout: matrixTimeout });
    if (!response.ok) {
      const errorText = await response.text();
      console.error(
        `[MatrixGen] OSRM /table HTTP error: ${response.status}. Body: ${errorText}`
      );
      throw new Error(
        `OSRM table service error for matrix: ${response.status}`
      );
    }
    const data = await response.json();
    if (data.code !== "Ok" || !data.durations || !data.distances) {
      console.error(
        "[MatrixGen] Invalid OSRM table response structure or error code:",
        data
      );
      throw new Error(
        "Invalid OSRM table response for matrix (structure or code)"
      );
    }
    const pointMap = [
      { empCode: "FACILITY", isFacility: true, ...facilityLocation },
      ...locationsForMatrix,
    ];
    return {
      distanceMatrix: data.distances,
      durationMatrix: data.durations,
      pointMap: pointMap,
    };
  } catch (error) {
    console.error(
      "[MatrixGen] Failed to generate distance/duration matrix:",
      error
    );
    throw error; // Re-throw to be caught by caller
  }
}

async function solveZoneWithORTools(
  zoneEmployees,
  facilityData,
  vehicleCapacity,
  maxRouteDurationSeconds,
  pickupTimePerEmployee,
  tripType,
  zoneName,
  // New parameter to control num_vehicles for single route optimization
  forceSingleVehicleOptimization = false
) {
  const currentZoneNameForLogging =
    zoneName || zoneEmployees[0]?.zone || "UNKNOWN_ZONE_IN_SOLVER";
  if (!zoneEmployees || zoneEmployees.length === 0) {
    console.warn(
      `[OR-TOOLS SOLVER] No employees for zone "${currentZoneNameForLogging}" to solve.`
    );
    return { routes: [], droppedEmployees: [] };
  }

  console.log(
    `\n[OR-TOOLS SOLVER] Solving for zone: "${currentZoneNameForLogging}" with ${zoneEmployees.length} employees. VehCap: ${vehicleCapacity}, MaxRouteDur: ${maxRouteDurationSeconds}s, PickupTime: ${pickupTimePerEmployee}s.`
  );

  const facilityLocation = { lat: facilityData.geoY, lng: facilityData.geoX };
  let pointMapForCurrentZone = [];

  try {
    const matrixData = await generateDistanceDurationMatrix(
      zoneEmployees,
      facilityLocation
    );
    const { distanceMatrix, durationMatrix } = matrixData;
    pointMapForCurrentZone = matrixData.pointMap;

    if (
      !distanceMatrix ||
      distanceMatrix.length === 0 ||
      (distanceMatrix.length > 0 && distanceMatrix[0].length === 0)
    ) {
      console.warn(
        `[OR-TOOLS SOLVER] Empty/invalid distance matrix for zone "${currentZoneNameForLogging}".`
      );
      return { routes: [], droppedEmployees: zoneEmployees }; // Return all as dropped
    }
    if (pointMapForCurrentZone.length !== distanceMatrix.length) {
      console.error(
        `[OR-TOOLS SOLVER] Mismatch pointMap length and matrix dimensions for zone "${currentZoneNameForLogging}"!`
      );
      return { routes: [], droppedEmployees: zoneEmployees };
    }

    const numCustomers = zoneEmployees.length;
    let numVehiclesForSolver;
    if (forceSingleVehicleOptimization) {
      numVehiclesForSolver = 1;
      console.log(
        `[OR-TOOLS SOLVER] Optimizing pre-formed route for zone "${currentZoneNameForLogging}", using numVehiclesForSolver: 1`
      );
    } else {
      numVehiclesForSolver = numCustomers > 0 ? numCustomers : 1; // Ample vehicles for initial zone solve
      console.log(
        `[OR-TOOLS SOLVER] Solving for zone "${currentZoneNameForLogging}", using numVehiclesForSolver: ${numVehiclesForSolver}`
      );
    }

    const demands = [0, ...zoneEmployees.map(() => 1)];
    const serviceTimes = [0, ...zoneEmployees.map(() => pickupTimePerEmployee)];

    const orToolsInput = {
      distance_matrix: distanceMatrix,
      duration_matrix: durationMatrix,
      num_vehicles: numVehiclesForSolver,
      vehicle_capacities: Array(numVehiclesForSolver).fill(vehicleCapacity),
      demands: demands,
      depot_index: 0,
      max_route_duration: maxRouteDurationSeconds,
      service_times: serviceTimes,
      allow_dropping_visits:
        facilityData.profile?.allowDroppingVisitsForProblematicZones || true, // Default to true
      drop_visit_penalty: facilityData.profile?.dropPenalty || 36000, // e.g., 10 hours in seconds,
      facility_coords: [facilityLocation.lat, facilityLocation.lng], // Pass lat, lng
      trip_type: tripType.toUpperCase(), // Pass 'PICKUP' or 'DROPOFF'
      // Optional: Add a weight for the penalty
      direction_penalty_weight:
        facilityData.profile?.directionPenaltyWeight || 2.0, // Default to 1.0, tune this!
    };

    const pythonExecutable = "python";
    const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py"); // Corrected path

    if (!fs.existsSync(scriptPath)) {
      console.error(
        `[OR-TOOLS SOLVER] Python solver script not found at: ${scriptPath}`
      );
      throw new Error(`Solver script not found: ${scriptPath}`);
    }

    const pythonProcess = spawn(pythonExecutable, [scriptPath]);
    // ... (rest of spawn, stdout/stderr, Promise logic as in your last correct version) ...
    // Ensure the parsing of solution.dropped_node_indices is present
    // and that you map these indices back to employee objects.
    let scriptOutput = "";
    let scriptError = "";

    pythonProcess.stdin.write(JSON.stringify(orToolsInput));
    pythonProcess.stdin.end();

    pythonProcess.stdout.on("data", (data) => {
      scriptOutput += data.toString();
    });
    pythonProcess.stderr.on("data", (data) => {
      const errData = data.toString();
      console.error(
        `[OR-TOOLS Python stderr FOR ZONE ${currentZoneNameForLogging}]: ${errData}`
      );
      scriptError += errData;
    });

    return new Promise((resolve, reject) => {
      pythonProcess.on("close", (code) => {
        let solution = null;
        let parsedSuccessfully = false;
        let parseErrorDetail = null;

        if (scriptOutput && scriptOutput.trim() !== "") {
          try {
            let lastBraceIndex = scriptOutput.lastIndexOf("}");
            if (lastBraceIndex !== -1) {
              let openBraceCount = 0;
              let firstBraceIndex = -1;
              for (let i = lastBraceIndex; i >= 0; i--) {
                if (scriptOutput[i] === "}") openBraceCount++;
                else if (scriptOutput[i] === "{") {
                  openBraceCount--;
                  if (openBraceCount === 0) {
                    firstBraceIndex = i;
                    break;
                  }
                }
              }
              if (firstBraceIndex !== -1) {
                const potentialJsonString = scriptOutput.substring(
                  firstBraceIndex,
                  lastBraceIndex + 1
                );
                solution = JSON.parse(potentialJsonString);
                parsedSuccessfully = true;
              }
            }
            if (!parsedSuccessfully) {
              const lines = scriptOutput.trim().split("\n");
              const lastLine = lines[lines.length - 1].trim();
              if (lastLine.startsWith("{") && lastLine.endsWith("}")) {
                solution = JSON.parse(lastLine);
                parsedSuccessfully = true;
              } else {
                throw new Error("Last line not valid JSON.");
              }
            }
          } catch (e) {
            parseErrorDetail = e.message;
            console.error(
              `[OR-TOOLS SOLVER] Error parsing Python stdout for zone "${currentZoneNameForLogging}":`,
              e,
              "\nRaw stdout:\n",
              scriptOutput
            );
          }
        }

        if (code !== 0) {
          let pyErrMsg = `Python script (zone "${currentZoneNameForLogging}") exit ${code}.`;
          if (scriptError) {
            try {
              const eObj = JSON.parse(scriptError);
              if (eObj.error)
                pyErrMsg = `Python error (zone "${currentZoneNameForLogging}"): ${
                  eObj.error
                } - ${eObj.details || ""}`;
              else pyErrMsg += ` Stderr: ${scriptError}`;
            } catch (e) {
              pyErrMsg += ` Stderr: ${scriptError}`;
            }
          }
          return reject(new Error(pyErrMsg));
        }
        if (!parsedSuccessfully || !solution)
          return reject(
            new Error(
              `Failed to parse OR-Tools solution for zone "${currentZoneNameForLogging}". Parse error: ${
                parseErrorDetail || "Unknown"
              }. Output: ${scriptOutput.substring(0, 500)}`
            )
          );
        if (solution.error)
          return reject(
            new Error(
              `OR-Tools solver error (zone "${currentZoneNameForLogging}"): ${solution.error}`
            )
          );

        const orRoutes = [];
        const solutionDroppedIndices = solution.dropped_node_indices || [];
        const droppedEmployees = solutionDroppedIndices
          .map((nodeIdx) => {
            if (nodeIdx > 0 && nodeIdx < pointMapForCurrentZone.length)
              return pointMapForCurrentZone[nodeIdx];
            console.error(
              `[OR-TOOLS] Invalid dropped_node_index ${nodeIdx} for pointMap length ${pointMapForCurrentZone.length}`
            );
            return null;
          })
          .filter(Boolean);

        if (solution.routes && Array.isArray(solution.routes)) {
          solution.routes.forEach((routeNodeIndices) => {
            if (routeNodeIndices.length > 0) {
              const currentRouteEmployees = routeNodeIndices
                .map((nodeIndex) => {
                  if (nodeIndex === 0) return null;
                  if (nodeIndex >= pointMapForCurrentZone.length) {
                    console.error(
                      `[OR-TOOLS] Route nodeIndex ${nodeIndex} out of bounds for pointMap length ${pointMapForCurrentZone.length}`
                    );
                    return null;
                  }
                  return pointMapForCurrentZone[nodeIndex];
                })
                .filter((emp) => emp != null && !emp.isFacility);

              if (currentRouteEmployees.length > 0) {
                orRoutes.push({
                  employees: currentRouteEmployees,
                  vehicleCapacity,
                  guardNeeded: false,
                  zone: currentZoneNameForLogging,
                  tripType,
                });
              }
            }
          });
        }
        console.log(
          `[OR-TOOLS SOLVER] Processed ${orRoutes.length} routes, ${droppedEmployees.length} dropped for zone: "${currentZoneNameForLogging}".`
        );
        resolve({ routes: orRoutes, droppedEmployees }); // Return dropped employees
      });
      pythonProcess.on("error", (err) => {
        console.error(
          `[OR-TOOLS SOLVER] Failed to start Python subprocess for zone "${currentZoneNameForLogging}".`,
          err
        );
        reject(err);
      });
    });
  } catch (error) {
    console.error(
      `[OR-TOOLS SOLVER] Critical error in solveZoneWithORTools for zone "${currentZoneNameForLogging}":`,
      error
    );
    return { routes: [], droppedEmployees: [...zoneEmployees] }; // All employees dropped on critical error
  }
}

// routeGenerationService.js

// ... (all other existing helper functions: isOsrmAvailable, decodePolyline, etc.)
// ... (isNightShiftForGuard - ensure this is defined as previously discussed)
// ... (processEmployeeBatch - ensure this is the latest version we worked on)
// ... (solveZoneWithORTools, reOptimizeSwappedRouteWithORTools)
// ... (calculateRouteDetails, updateRouteWithDetails, assignErrorState, etc.)

// routeGenerationService.js

// Make sure all your helper functions are defined above this:
// isOsrmAvailable, decodePolyline, encodePolyline, toRadians, calculateDistance,
// haversineDistance, isPointInPolygon, calculateAngle, angleDifference,
// loadZonesData, assignEmployeesToZones, findZoneGroups, getZoneCapacity,
// isNightShiftForGuard, calculateRouteDetails, reOptimizeSwappedRouteWithORTools,
// processEmployeeBatch, solveZoneWithORTools, handleGuardRequirements,
// validateSwap, findComplexSwap, assignErrorState, updateRouteWithDetails,
// calculatePickupTimes, calculateRouteStatistics, createSimplifiedResponse, createEmptyResponse

async function generateRoutes(data) {
  try {
    const {
      employees,
      facility,
      shiftTime,
      date,
      profile,
      saveToDatabase = false,
      pickupTimePerEmployee = 180,
      reportingTime = 0,
      guard = false,
      tripType = "PICKUP",
    } = data;

    if (!employees?.length) throw new Error("Employee data is required");
    if (!facility?.geoX || !facility?.geoY) throw new Error("Valid facility data required");
    if (!date || !shiftTime || !profile) throw new Error("Missing required parameters");

    const osrmAvailable = await isOsrmAvailable();
    if (!osrmAvailable) throw new Error("OSRM routing service unavailable");

    const useZones = profile.zoneBasedRouting !== undefined ? !!profile.zoneBasedRouting : true;
    let employeesByZone = {};
    const removedForGuardByZone = {};

    const ensureSpecialFlags = (emp) => ({
        ...emp,
        isMedical: emp.isMedical || false,
        isPWD: emp.isPWD || false,
    });

    if (useZones) {
      let zones = data.zones || [];
      if (!zones.length && ZONES_DATA_FILE) {
        try { zones = await loadZonesData(); if (!zones.length) console.warn("No zones data loaded."); }
        catch (err) { console.error(`Failed to load zones: ${err.message}.`); }
      }
      // Ensure employees passed to assignEmployeesToZones have the flags
      employeesByZone = assignEmployeesToZones(employees.map(ensureSpecialFlags), zones);
      if (Object.keys(employeesByZone).length === 0 && employees.length > 0) {
        if (!employeesByZone["DEFAULT_ZONE"]) employeesByZone["DEFAULT_ZONE"] = [];
        employees.forEach((emp) => {
          if (!Object.values(employeesByZone).flat().find((e) => e.empCode === emp.empCode)) {
            employeesByZone["DEFAULT_ZONE"].push({
              ...ensureSpecialFlags(emp),
              zone: "DEFAULT_ZONE",
              location: { lat: emp.geoY, lng: emp.geoX },
            });
          }
        });
        // if (employeesByZone["DEFAULT_ZONE"].length > 0) console.log("Some employees assigned to DEFAULT_ZONE.");
      }
    } else {
      employeesByZone = {
        GLOBAL: employees.map((emp) => ({
          ...ensureSpecialFlags(emp),
          zone: "GLOBAL",
          location: { lat: emp.geoY, lng: emp.geoX },
        })),
      };
    }

    const routeData = {
      uuid: data.uuid || uuidv4(), date, shift: shiftTime, tripType: tripType.toUpperCase(),
      facility, profile, employeeData: employees, routeData: [],
    };

    const processedZones = new Set();
    const { zonePairingMatrix = {}, maxDuration: profileMaxDuration = 7200 } = profile;
    let totalRouteCount = 0;
    let finalTotalSwappedRoutes = 0;
    const allInitiallyFormedRoutes = [];
    let unroutedByOrTools = [];

    const isDropoff = tripType.toLowerCase() === "dropoff";
    const facilityCoordinates = [facility.geoY, facility.geoX];

    const processZoneOrGroup = async (empsInScope, zoneIdentifier, effectiveMaxCapacity) => {
      if (empsInScope.length === 0) return;
      const { routes: batchRoutes } = await processEmployeeBatch(
        empsInScope, effectiveMaxCapacity, facility, tripType,
        profileMaxDuration, pickupTimePerEmployee, guard,
      );
      batchRoutes.forEach((route) => {
        route.zone = zoneIdentifier;
        allInitiallyFormedRoutes.push(route);
      });
    };

    if (profile.zoneClubbing) {
      const zoneGroups = findZoneGroups(zonePairingMatrix);
      for (const group of zoneGroups) {
        const clubbedZoneName = group.join("-");
        const combinedEmployees = group.flatMap((zn) => employeesByZone[zn] || []).filter((e) => e.location);
        const maxCap = Math.max(...group.map((z) => getZoneCapacity(z, profile)), 1);
        await processZoneOrGroup(combinedEmployees, clubbedZoneName, maxCap);
        group.forEach((z) => processedZones.add(z));
      }
    }
    for (const [zoneName, zoneEmpList] of Object.entries(employeesByZone)) {
      if (processedZones.has(zoneName)) continue;
      const currentZoneEmployees = (zoneEmpList || []).filter((e) => e.location);
      const maxCap = getZoneCapacity(zoneName, profile);
      await processZoneOrGroup(currentZoneEmployees, zoneName, maxCap);
    }

    const allOptimizedOrToolsRoutes = [];
    for (const initialRoute of allInitiallyFormedRoutes) {
      if (!initialRoute.employees || initialRoute.employees.length === 0) continue;
      try {
        const { routes: orToolsSolvedRouteList, droppedEmployees } =
          await solveZoneWithORTools(
            initialRoute.employees, facility, initialRoute.vehicleCapacity,
            profileMaxDuration, pickupTimePerEmployee, tripType,
            initialRoute.zone, true,
          );
        if (droppedEmployees && droppedEmployees.length > 0) unroutedByOrTools.push(...droppedEmployees);
        if (orToolsSolvedRouteList && orToolsSolvedRouteList.length > 0) {
          allOptimizedOrToolsRoutes.push({ ...initialRoute, employees: orToolsSolvedRouteList[0].employees });
        } else {
          allOptimizedOrToolsRoutes.push(initialRoute);
        }
      } catch (error) {
        console.error(`  [OR-Tools Stage] Error optimizing route for zone ${initialRoute.zone}: ${error.message}. Using original order.`);
        allOptimizedOrToolsRoutes.push(initialRoute);
      }
    }

    const finalProcessedRoutes = [];
    for (const route of allOptimizedOrToolsRoutes) {
      try {
        totalRouteCount++;
        route.routeNumber = totalRouteCount;
        let routeModifiedByGuardSwap = false;
        route.guardNeeded = false;

        if (!route.employees || route.employees.length === 0) {
          assignErrorState(route, "Route empty before OSRM in post-processing");
          continue; // Don't add to finalProcessedRoutes
        }

        const routeCoordinates = route.employees.map((emp) => [emp.location.lat, emp.location.lng]);
        const currentAllCoordinates = isDropoff
          ? [facilityCoordinates, ...routeCoordinates]
          : [...routeCoordinates, facilityCoordinates];
        let currentRouteDetails = await calculateRouteDetails(
          currentAllCoordinates, route.employees, pickupTimePerEmployee, tripType,
        );

        if (currentRouteDetails.error) {
          assignErrorState(route, `OSRM /trip failed: ${currentRouteDetails.error}`);
          continue; // Don't add to finalProcessedRoutes
        }
        updateRouteWithDetails(route, currentRouteDetails);

        let routeActuallyNeedsExternalGuard = false;
        let performReOptimization = false;
        const nightShiftActive = isNightShiftForGuard(shiftTime, tripType, profile);

        if (guard && route.employees.length > 0 && nightShiftActive) {
          const guardSwapResult = await handleGuardRequirements(
            route, isDropoff, facility, pickupTimePerEmployee,
          );
          if (guardSwapResult.swapped && guardSwapResult.routeDetails && !guardSwapResult.routeDetails.error) {
            routeModifiedByGuardSwap = true;
            finalTotalSwappedRoutes++;
            route.guardNeeded = false;
            updateRouteWithDetails(route, guardSwapResult.routeDetails);
            currentRouteDetails = guardSwapResult.routeDetails;
            performReOptimization = true;
          } else if (guardSwapResult.guardNeeded) {
            routeActuallyNeedsExternalGuard = true;
            route.guardNeeded = true;
          } else {
            const critIdx = isDropoff ? route.employees.length - 1 : 0;
            if (route.employees.length > 0 && route.employees[critIdx].gender === "F" && !route.employees.some(e => e.gender === "M")) {
              routeActuallyNeedsExternalGuard = true;
              route.guardNeeded = true;
            } else {
              route.guardNeeded = false;
            }
          }
        } else if (guard && route.employees.length > 0 && !nightShiftActive) {
          route.guardNeeded = false;
        }

        if (performReOptimization) {
          const capacityForReOpt = route.isSpecialNeedsRoute ? 2 : route.vehicleCapacity;
          const reOptResult = await reOptimizeSwappedRouteWithORTools(
            { ...route, vehicleCapacity: capacityForReOpt },
            facility, pickupTimePerEmployee,
          );
          if (reOptResult.reOptimized && reOptResult.employees.length > 0) {
            route.employees = reOptResult.employees;
            const reOptRouteCoordinates = route.employees.map(emp => [emp.location.lat, emp.location.lng]);
            const reOptAllCoordinates = isDropoff ? [facilityCoordinates, ...reOptRouteCoordinates] : [...reOptRouteCoordinates, facilityCoordinates];
            currentRouteDetails = await calculateRouteDetails(reOptAllCoordinates, route.employees, pickupTimePerEmployee, tripType);
            if (currentRouteDetails.error) {
              assignErrorState(route, `OSRM /trip failed after re-optimization: ${currentRouteDetails.error}`);
            } else {
              updateRouteWithDetails(route, currentRouteDetails);
            }
          }
        }

        if (routeActuallyNeedsExternalGuard) {
          let passengerCapacity;
          const capacityBasisForGuardLogic = route.vehicleCapacity;

          if (route.isSpecialNeedsRoute) { // Using the correct flag
            passengerCapacity = 1;
          } else {
            passengerCapacity = Math.max(1, capacityBasisForGuardLogic - 1);
          }

          if (route.employees.length > passengerCapacity) {
            const numToRemove = route.employees.length - passengerCapacity;
            for (let i = 0; i < numToRemove; i++) {
              if (route.employees.length === 0) break;
              const removedEmp = isDropoff ? route.employees.shift() : route.employees.pop();
              if (removedEmp) {
                if (!removedForGuardByZone[route.zone]) removedForGuardByZone[route.zone] = [];
                removedForGuardByZone[route.zone].push(removedEmp);
                // Use the module-level isSpecialNeedsUser here
                if (isSpecialNeedsUser(removedEmp) && !route.employees.some(isSpecialNeedsUser)) {
                  route.isSpecialNeedsRoute = false; // Update the correct flag
                }
              }
            }
            if (route.employees.length > 0) {
                const newCoords = route.employees.map(e => [e.location.lat, e.location.lng]);
                const newAllCoordsForGuardTrim = isDropoff ? [facilityCoordinates, ...newCoords] : [...newCoords, facilityCoordinates];
                const recalcDetailsAfterGuardTrim = await calculateRouteDetails(newAllCoordsForGuardTrim, route.employees, pickupTimePerEmployee, tripType);
                if (!recalcDetailsAfterGuardTrim.error) {
                    updateRouteWithDetails(route, recalcDetailsAfterGuardTrim);
                    currentRouteDetails = recalcDetailsAfterGuardTrim;
                } else {
                    assignErrorState(route, `OSRM failed after guard removal: ${recalcDetailsAfterGuardTrim.error}`);
                }
            } else {
                assignErrorState(route, "No employees after guard removal");
            }
          }
          route.vehicleCapacity = passengerCapacity;
        }

        if (route.employees.length > 0 && !route.error) {
          calculatePickupTimes(route, shiftTime, pickupTimePerEmployee, reportingTime);
          if (profileMaxDuration && route.routeDetails && route.routeDetails.duration > profileMaxDuration) {
            route.durationExceeded = true;
          }
        } else if (!route.error && route.employees.length === 0) {
           assignErrorState(route, "Route became empty after post-processing");
        }

        route.swapped = routeModifiedByGuardSwap;
        if (!route.error && route.employees.length > 0) {
            finalProcessedRoutes.push(route);
        }
      } catch (error) {
        console.error(`Critical error in post-processing loop for route ${route?.routeNumber || 'UNKNOWN'}:`, error);
        if (route) {
          assignErrorState(route, `Post-processing loop critical error: ${error.message}`);
        }
      }
    }

    routeData.routeData = [...finalProcessedRoutes];

    let collectedUnroutedForReinsertion = [...unroutedByOrTools];
    for (const empList of Object.values(removedForGuardByZone)) {
      collectedUnroutedForReinsertion.push(...empList);
    }
    const potentiallyUnroutedMap = new Map(
      collectedUnroutedForReinsertion.map((emp) => [emp.empCode, emp])
    );
    const successfullyRoutedEmpCodesInMainPass = new Set();
    finalProcessedRoutes.forEach(route => {
        if (!route.error && route.employees) {
            route.employees.forEach(emp => successfullyRoutedEmpCodesInMainPass.add(emp.empCode));
        }
    });
    const finalUnroutedEmployees = Array.from(potentiallyUnroutedMap.values()).filter(
        emp => !successfullyRoutedEmpCodesInMainPass.has(emp.empCode)
    );

    // console.log(`DEBUG: Employees TRULY unrouted and going into re-insertion: ${finalUnroutedEmployees.length} - ${finalUnroutedEmployees.map(e=>e.empCode).join(',')}`);
    if (finalUnroutedEmployees.length > 0) {
      const { routes: newRoutesForUnrouted } = await processEmployeeBatch(
        finalUnroutedEmployees, getZoneCapacity("DEFAULT_ZONE", profile),
        facility, tripType, profileMaxDuration, pickupTimePerEmployee, guard,
      );
      for (const newRoute of newRoutesForUnrouted) {
          totalRouteCount++;
          newRoute.routeNumber = totalRouteCount;
          newRoute.zone = newRoute.zone || "UNROUTED_REINSERTED";
          if (newRoute.employees.length > 0) {
              const newRouteCoords = newRoute.employees.map((e) => [e.location.lat, e.location.lng]);
              const newAllCoords = isDropoff ? [facilityCoordinates, ...newRouteCoords] : [...newRouteCoords, facilityCoordinates];
              const finalDetails = await calculateRouteDetails(newAllCoords, newRoute.employees, pickupTimePerEmployee, tripType);
              if (!finalDetails.error) {
                  updateRouteWithDetails(newRoute, finalDetails);
                  calculatePickupTimes(newRoute, shiftTime, pickupTimePerEmployee, reportingTime);
                  if (profileMaxDuration && newRoute.routeDetails && newRoute.routeDetails.duration > profileMaxDuration) {
                      newRoute.durationExceeded = true;
                  }
              } else {
                  assignErrorState(newRoute, `OSRM failed for re-inserted unrouted: ${finalDetails.error}`);
              }
          } else {
              assignErrorState(newRoute, "Re-inserted unrouted route has no employees");
          }
          if (!newRoute.error && newRoute.employees.length > 0) {
            routeData.routeData.push(newRoute);
          }
      }
    }

    // Debugging block for duplicates (can be removed after fixing)
    // const allRoutedEmployeeCodes = [];
    // routeData.routeData.forEach(route => {
    //     if (!route.error && route.employees) {
    //         route.employees.forEach(emp => { allRoutedEmployeeCodes.push(emp.empCode); });
    //     }
    // });
    // const codeCounts = {};
    // let duplicatesFound = false;
    // allRoutedEmployeeCodes.forEach(code => {
    //     codeCounts[code] = (codeCounts[code] || 0) + 1;
    //     if (codeCounts[code] > 1) {
    //         console.error(`DUPLICATE ROUTED (after re-insertion): Employee ${code} is in ${codeCounts[code]} routes!`);
    //         duplicatesFound = true;
    //     }
    // });
    // if (duplicatesFound) console.error("ERROR: Duplicate employee assignments found in final routes after re-insertion.");
    // else console.log("INFO: No duplicate employee assignments found in final routes after re-insertion.");
    // const uniqueRoutedEmployees = new Set(allRoutedEmployeeCodes);
    // console.log(`DEBUG: Unique routed employee count from final routes (after re-insertion): ${uniqueRoutedEmployees.size}`);

    const stats = calculateRouteStatistics(routeData, employees.length);
    const response = createSimplifiedResponse({
      ...routeData, ...stats, totalSwappedRoutes: finalTotalSwappedRoutes,
    });

    if (saveToDatabase) { /* console.log("Simulating save to database"); */ }
    return response;

  } catch (error) {
    console.error("Top-level generateRoutes error:", error);
    const inputData = typeof data === "object" && data !== null ? data : {};
    return createEmptyResponse({
        uuid: inputData.uuid, date: inputData.date, shiftTime: inputData.shiftTime,
        tripType: inputData.tripType, employees: inputData.employees,
    });
  }
}

// Node.js: routeGenerationService.js

// ... (other existing imports and helper functions like isOsrmAvailable, decodePolyline, etc.)

function formatTime(date) {
  if (!(date instanceof Date) || isNaN(date.getTime())) {
    // More robust date validation
    console.error("formatTime: Invalid date object received", date);
    return "Invalid Time";
  }
  return date.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  });
}

function calculatePickupTimes(
  route,
  shiftTime,
  pickupTimePerEmployee,
  reportingTimeSeconds = 0 // This parameter is present, ensure it's used if intended
) {
  try {
    if (!route || !route.employees || !route.employees.length || !shiftTime) {
      console.error("calculatePickupTimes: Invalid input parameters.", {
        routeExists: !!route,
        employeesExist: !!route?.employees,
        shiftTimeExists: !!shiftTime,
      });
      throw new Error("Invalid input parameters for calculatePickupTimes");
    }

    const timeStr = shiftTime.toString().padStart(4, "0");
    const hours = parseInt(timeStr.substring(0, 2), 10);
    const minutes = parseInt(timeStr.substring(2, 4), 10);

    if (
      isNaN(hours) ||
      isNaN(minutes) ||
      hours < 0 ||
      hours > 23 ||
      minutes < 0 ||
      minutes > 59
    ) {
      throw new Error(`Invalid shift time format: ${shiftTime}`);
    }

    const facilityTargetTime = new Date();
    facilityTargetTime.setHours(hours, minutes, 0, 0);

    // Adjust for reporting time if it's a pickup scenario and reporting time is significant
    // For pickups, the vehicle should arrive at the facility *at* shiftTime.
    // If reportingTimeSeconds means employees need to be at facility *before* shiftTime,
    // then facilityTargetTime should be shiftTime - reportingTimeSeconds.
    // For now, assuming facilityTargetTime is the actual shift start.
    // If reportingTime is for employees to be ready *before* pickup, it's handled by their availability.

    const isDropoff = route.tripType?.toLowerCase() === "dropoff";
    let currentTime = new Date(facilityTargetTime);

    if (!isDropoff) {
      // For PICKUP:
      // The time the vehicle arrives at the facility with all employees.
      // If reportingTimeSeconds means employees must be at facility X seconds *before* shiftTime,
      // then the target arrival at facility is shiftTime - reportingTimeSeconds.
      // Let's assume shiftTime is the target arrival at facility for now.
      let targetFacilityArrivalTime = new Date(facilityTargetTime);
      if (reportingTimeSeconds > 0) {
        targetFacilityArrivalTime.setSeconds(
          targetFacilityArrivalTime.getSeconds() - reportingTimeSeconds
        );
      }
      route.facilityArrivalTime = formatTime(targetFacilityArrivalTime);
      currentTime = new Date(targetFacilityArrivalTime); // Start calculations from this target arrival

      for (let i = route.employees.length - 1; i >= 0; i--) {
        const employee = route.employees[i];
        // Leg duration is from previous stop (or facility for the last picked employee) to this employee
        // The OSRM trip /trip API returns legs where legs[i] is the travel from waypoint[i] to waypoint[i+1]
        // For pickup, employees are waypoints 0 to N-1, facility is N.
        // So, route.routeDetails.legs[i] is travel from employee i to employee i+1 (or employee N-1 to facility)
        // When calculating backwards:
        // currentTime is arrival at (i+1) or facility.
        // Subtract travel from i to (i+1) -> this is employee[i]'s dropoff time at point (i+1)
        // Subtract service time at i -> this is employee[i]'s pickup time.

        // Leg from employee i to employee i+1 (or facility if i is the last employee)
        const legToNextStopOrFacility = route.routeDetails?.legs?.[i]; // leg[i] is from emp[i] to emp[i+1] or facility
        const legDuration =
          (legToNextStopOrFacility?.duration || 0) *
          (1 + TRAFFIC_BUFFER_PERCENTAGE);

        // CurrentTime is arrival at stop i+1 (or facility)
        // Subtract travel time from emp i to emp i+1
        currentTime.setSeconds(currentTime.getSeconds() - legDuration);
        // CurrentTime is now arrival time at emp i's location (if they were dropped off)
        // or departure time from emp i's location after pickup.

        // Subtract service time (pickupTimePerEmployee)
        currentTime.setSeconds(
          currentTime.getSeconds() - pickupTimePerEmployee
        );
        // CurrentTime is now the actual pickup time for employee i
        employee.pickupTime = formatTime(currentTime);
      }
    } else {
      // For DROPOFF:
      // currentTime is facility departure time.
      // If reportingTimeSeconds has a meaning for dropoff (e.g. buffer before first drop), adjust here.
      route.facilityDepartureTime = formatTime(currentTime);

      for (let i = 0; i < route.employees.length; i++) {
        const employee = route.employees[i];
        // Leg from previous stop (or facility for the first dropped employee) to this employee
        // For dropoff, facility is waypoint 0, employees are 1 to N.
        // route.routeDetails.legs[i] is travel from facility to emp[0] (if i=0)
        // or from emp[i-1] to emp[i] (if i>0)
        const legToThisEmployee = route.routeDetails?.legs?.[i];
        const legDuration =
          (legToThisEmployee?.duration || 0) * (1 + TRAFFIC_BUFFER_PERCENTAGE);

        // Add travel time to current employee
        currentTime.setSeconds(currentTime.getSeconds() + legDuration);
        // CurrentTime is now arrival time at emp i's location

        // Add service time (dropoff time)
        currentTime.setSeconds(
          currentTime.getSeconds() + pickupTimePerEmployee
        ); // Assuming pickupTimePerEmployee is also dropoff service time
        // CurrentTime is now the departure time from emp i's location (or final dropoff time)
        employee.dropoffTime = formatTime(currentTime);
        employee.pickupTime = employee.dropoffTime; // For consistency in ETA field
      }
    }
  } catch (error) {
    console.error("Time calculation error in calculatePickupTimes:", error);
    if (route && route.employees) {
      route.employees.forEach((emp) => {
        emp.pickupTime = "Error";
        emp.dropoffTime = "Error";
      });
    }
    if (route) {
      route.facilityArrivalTime = "Error";
      route.facilityDepartureTime = "Error";
    }
  }
}

async function handleGuardRequirements(
  route,
  isDropoff,
  facility, // Not directly used for this distance check, but good to have
  pickupTimePerEmployee, // Not directly used for this distance check
) {
  try {
    if (!route?.employees?.length || route.employees.length < 2) { // Need at least 2 to swap
      return { guardNeeded: false, swapped: false };
    }

    const checkIndex = isDropoff ? route.employees.length - 1 : 0;
    const criticalEmployee = route.employees[checkIndex];

    if (!criticalEmployee || criticalEmployee.gender !== "F") {
      return { guardNeeded: false, swapped: false };
    }

    const potentialMaleCandidates = route.employees.filter(
      (emp, index) => index !== checkIndex && emp.gender === "M"
    );

    if (potentialMaleCandidates.length === 0) {
      // console.log(`Guard needed for route ${route.routeNumber}: No male employees in route to consider for swap.`);
      return { guardNeeded: true, swapped: false };
    }

    // Prepare coordinates for OSRM table request
    // Source will be the criticalEmployee
    // Destinations will be all potentialMaleCandidates
    const osrmCoordinates = [
      `${criticalEmployee.location.lng},${criticalEmployee.location.lat}`, // Source 0
      ...potentialMaleCandidates.map(
        (emp) => `${emp.location.lng},${emp.location.lat}` // Destinations 1 to N
      ),
    ];

    const sources = "0"; // Index of criticalEmployee in osrmCoordinates
    const destinations = potentialMaleCandidates
      .map((_, i) => i + 1) // Indices of male candidates in osrmCoordinates
      .join(";");

    const osrmTableUrl = `http://localhost:5000/table/v1/driving/${osrmCoordinates.join(
      ";"
    )}?sources=${sources}&destinations=${destinations}&annotations=distance`; // We only need distance for this check

    let osrmDistances = [];
    try {
      const response = await fetchApi(osrmTableUrl, { timeout: OSRM_PROBE_TIMEOUT_HEURISTIC }); // Use a reasonable timeout
      if (response.ok) {
        const data = await response.json();
        if (data.code === "Ok" && data.distances && data.distances.length > 0) {
          osrmDistances = data.distances[0]; // Distances from source 0 to all destinations
        } else {
          console.warn(`[handleGuardRequirements] OSRM /table error or no distances for route ${route.routeNumber}: ${data.code} - ${data.message || ''}`);
          // Fallback or error: if OSRM fails, we can't use road distance.
          // Option: Fallback to Haversine, or consider it a failure to find swappable candidate.
          // For now, let's treat OSRM failure as "no swappable candidate by road distance".
          return { guardNeeded: true, swapped: false };
        }
      } else {
        console.warn(`[handleGuardRequirements] OSRM /table HTTP error ${response.status} for route ${route.routeNumber}`);
        return { guardNeeded: true, swapped: false };
      }
    } catch (error) {
      console.error(`[handleGuardRequirements] OSRM /table fetch error for route ${route.routeNumber}:`, error);
      return { guardNeeded: true, swapped: false };
    }

    const validCandidates = [];
    potentialMaleCandidates.forEach((maleEmp, idx) => {
      const roadDistanceMeters = osrmDistances[idx]; // OSRM distances are in meters
      if (roadDistanceMeters != null) { // Check if OSRM could route between them
        const roadDistanceKm = roadDistanceMeters / 1000;
        if (roadDistanceKm <= MAX_SWAP_DISTANCE_KM) {
          validCandidates.push({
            employee: maleEmp,
            // Find original index in route.employees for the swap
            index: route.employees.findIndex(e => e.empCode === maleEmp.empCode),
            distance: roadDistanceKm, // Store road distance
          });
        }
      }
    });

    if (validCandidates.length === 0) {
      // console.log(`Guard needed for route ${route.routeNumber}: No suitable male swap candidates found within ${MAX_SWAP_DISTANCE_KM}km road distance.`);
      return { guardNeeded: true, swapped: false };
    }

    // Sort by actual road distance
    validCandidates.sort((a, b) => a.distance - b.distance);
    const bestCandidate = validCandidates[0];

    // Perform the swap
    const newEmployees = [...route.employees];
    // Ensure bestCandidate.index is valid and different from checkIndex
    if (bestCandidate.index === -1 || bestCandidate.index === checkIndex) {
        console.error(`[handleGuardRequirements] Error finding original index for best candidate or candidate is the critical employee.`);
        return { guardNeeded: true, swapped: false };
    }

    [newEmployees[checkIndex], newEmployees[bestCandidate.index]] = [
      newEmployees[bestCandidate.index],
      newEmployees[checkIndex],
    ];

    // Recalculate full route details with OSRM /trip for the swapped sequence
    const newRouteCoordinates = newEmployees.map((emp) => [emp.location.lat, emp.location.lng]);
    const facilityCoordsArray = [facility.geoY, facility.geoX]; // Assuming facility is passed correctly
    const allCoordinatesForTrip = isDropoff
      ? [facilityCoordsArray, ...newRouteCoordinates]
      : [...newRouteCoordinates, facilityCoordsArray];

    const routeDetailsAfterSwap = await calculateRouteDetails(
      allCoordinatesForTrip,
      newEmployees, // Pass the swapped employee list
      pickupTimePerEmployee, // This is for service time in calculateRouteDetails, not travel
      route.tripType,
    );

    if (routeDetailsAfterSwap.error) {
      console.warn(`Swap validation (OSRM /trip) failed for route ${route.routeNumber} after road distance swap: ${routeDetailsAfterSwap.error}`);
      // If the new sequence is unroutable by OSRM /trip, the swap is not viable.
      return { guardNeeded: true, swapped: false };
    }

    // console.log(`Successfully identified swap for route ${route.routeNumber} using road distance: ${criticalEmployee.empCode} with ${bestCandidate.employee.empCode}. Road Distance: ${bestCandidate.distance.toFixed(2)}km`);
    return {
      guardNeeded: false,
      swapped: true,
      routeDetails: routeDetailsAfterSwap,
    };

  } catch (error) {
    console.error(`Error in handleGuardRequirements (road distance) for route ${route?.routeNumber}:`, error);
    return { guardNeeded: true, swapped: false }; // Default to needing a guard on error
  }
}


async function validateSwap(
  route,
  emp1Index,
  emp2Index,
  facility,
  pickupTimePerEmployee
) {
  try {
    const newEmployees = [...route.employees];
    [newEmployees[emp1Index], newEmployees[emp2Index]] = [
      newEmployees[emp2Index],
      newEmployees[emp1Index],
    ];

    const newCoordinates = newEmployees.map((emp) => [
      emp.location.lat,
      emp.location.lng,
    ]);

    const facilityCoordinates = [facility.geoY, facility.geoX];
    const isDropoff = route.tripType?.toLowerCase() === "dropoff";

    const allCoordinates = isDropoff
      ? [facilityCoordinates, ...newCoordinates]
      : [...newCoordinates, facilityCoordinates];

    const routeDetails = await calculateRouteDetails(
      allCoordinates,
      newEmployees,
      pickupTimePerEmployee,
      route.tripType
    );

    if (routeDetails.error) {
      return { viable: false, routeDetails }; // Return details even on error for logging
    }

    const originalDuration = route.routeDetails?.duration || Infinity; // Use current route's duration
    const newDuration = routeDetails.totalDuration;
    const durationIncrease =
      newDuration > originalDuration
        ? (newDuration - originalDuration) / originalDuration
        : 0;

    return {
      viable: durationIncrease <= 0.2, // Allow up to 20% increase
      routeDetails,
      // newCoordinates: allCoordinates, // Not strictly needed by caller
    };
  } catch (error) {
    console.error("Swap validation error:", error);
    return { viable: false, error: error.message };
  }
}

async function findComplexSwap(
  route,
  criticalIndex,
  facility,
  pickupTimePerEmployee
) {
  // Placeholder for more complex swap logic (e.g., 3-way swaps, or considering multiple male candidates)
  return null;
}

function assignErrorState(route, message = "Unknown error") {
  if (!route) return;
  console.warn(
    `Assigning error state to route ${
      route.routeNumber || "UNKNOWN"
    }: ${message}`
  );
  route.employees = (route.employees || []).map((e, i) => ({
    ...e,
    order: i + 1,
    pickupTime: "Error",
    dropoffTime: "Error",
  }));
  route.encodedPolyline = "error_polyline";
  route.routeDetails = { distance: 0, duration: 0, legs: [] };
  route.swapped = false;
  route.error = true;
  route.errorMessage = message;
}

function updateRouteWithDetails(route, routeDetails) {
  if (!route || !routeDetails) return;
  if (routeDetails.error) {
    console.warn(
      `Not updating route ${route.routeNumber} with errored details: ${routeDetails.error}`
    );
    assignErrorState(
      route,
      `Failed to update with details: ${routeDetails.error}`
    );
    return;
  }
  route.employees = routeDetails.employees;
  route.encodedPolyline = routeDetails.encodedPolyline;
  route.routeDetails = {
    distance: routeDetails.totalDistance,
    duration: routeDetails.totalDuration,
    legs: routeDetails.legs,
    geometry: routeDetails.geometry,
  };
  route.error = false;
  route.errorMessage = undefined;
}

function calculateRouteStatistics(routeData, totalEmployeesInput) {
  const validRoutes = routeData.routeData.filter(
    (route) => !route.error && route.employees?.length > 0
  );
  const totalValidRoutes = validRoutes.length;
  const totalRoutedEmployees = validRoutes.reduce(
    (sum, route) => sum + route.employees.length,
    0
  );

  const averageOccupancy =
    totalValidRoutes > 0 ? totalRoutedEmployees / totalValidRoutes : 0;

  let totalDistanceSum = 0;
  let totalDurationSum = 0;

  validRoutes.forEach((route) => {
    if (
      route.routeDetails?.duration !== Infinity &&
      route.routeDetails?.distance !== Infinity
    ) {
      totalDistanceSum += route.routeDetails?.distance || 0;
      totalDurationSum += route.routeDetails?.duration || 0;
    }
  });

  return {
    totalEmployees: totalEmployeesInput,
    totalRoutedEmployees,
    totalRoutes: totalValidRoutes,
    averageOccupancy: parseFloat(averageOccupancy.toFixed(2)),
    routeDetails: {
      totalDistance: parseFloat((totalDistanceSum / 1000).toFixed(2)), // km
      totalDuration: parseFloat(totalDurationSum.toFixed(2)), // seconds
    },
  };
}

// In routeGenerationService.js

function createSimplifiedResponse(routeData) {
  return {
    uuid: routeData.uuid,
    date: routeData.date,
    shift: routeData.shift,
    tripType: routeData.tripType === "PICKUP" ? "P" : "D", // Already good
    totalEmployees: routeData.totalEmployees,
    totalRoutedEmployees: routeData.totalRoutedEmployees,
    totalRoutes: routeData.totalRoutes,
    averageOccupancy: routeData.averageOccupancy,
    overallRouteDetails: routeData.routeDetails,
    totalSwappedRoutes: routeData.totalSwappedRoutes,
    routes: routeData.routeData
        .filter(route => !route.error && route.employees?.length > 0)
        .map((route) => {
            // route.guardNeeded is now the definitive flag for whether an external guard is assigned
            const guardAssigned = route.guardNeeded || false;

            // Occupancy calculation: number of employees + 1 if a guard is assigned
            // (assuming guard is not already an employee object)
            const occupancy = (route.employees?.length || 0) + (guardAssigned ? 1 : 0);

            // vehicleCapacity on the route object should be the *effective passenger capacity*
            // after medical and guard rules have been applied by processEmployeeBatch and generateRoutes.
            // For a special needs route, this would be 2 (or 1 if guard is also present).
            // For a regular route, it's physical_capacity (or physical_capacity - 1 if guard is present).
            const reportedVehicleCapacity = route.vehicleCapacity;

            return {
                routeNumber: route.routeNumber,
                zone: route.zone,
                vehicleCapacity: reportedVehicleCapacity, // This should be the final effective passenger capacity
                guard: guardAssigned,
                swapped: route.swapped || false,
                durationExceeded: route.durationExceeded || false,
                uniqueKey: route.uniqueKey,
                isSpecialNeedsRoute: route.isSpecialNeedsRoute || false, // <<< USE THE NEW FLAG
                // isMedicalRoute: route.isMedicalRoute, // <<< REMOVE OR RENAME if isSpecialNeedsRoute replaces it
                distance: parseFloat(((route.routeDetails?.distance || 0) / 1000).toFixed(2)),
                duration: parseFloat((route.routeDetails?.duration || 0).toFixed(2)),
                occupancy,
                encodedPolyline: route.encodedPolyline || "no_polyline",
                employees: (route.employees || []).map((emp, index) => ({
                    empCode: emp.empCode,
                    gender: emp.gender,
                    isMedical: emp.isMedical || false, // Include if useful for consumer
                    isPWD: emp.isPWD || false,       // Include if useful for consumer
                    eta: route.tripType?.toUpperCase() === "DROPOFF" ? emp.dropoffTime : emp.pickupTime,
                    order: emp.order !== undefined && emp.order >= 1 ? emp.order : index + 1,
                    geoX: emp.geoX,
                    geoY: emp.geoY,
                })),
            };
    }),
  };
}


function createEmptyResponse(data) {
  return {
    uuid: data.uuid || uuidv4(),
    date: data.date,
    shift: data.shiftTime,
    tripType: data.tripType?.toUpperCase() === "PICKUP" ? "P" : "D",
    totalEmployees: data.employees?.length || 0,
    totalRoutedEmployees: 0,
    totalRoutes: 0,
    averageOccupancy: 0,
    overallRouteDetails: { totalDistance: 0, totalDuration: 0 },
    totalSwappedRoutes: 0,
    routes: [],
  };
}

module.exports = {
  generateRoutes,
  isOsrmAvailable,
};
