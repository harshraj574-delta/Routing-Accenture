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

async function processEmployeeBatch(
  employees,
  maxCapacity, // Original physical capacity
  facility,
  tripType = "pickup",
  maxDuration,
  pickupTimePerEmployee,
  guard = false
) {
  const routes = [];
  const isDropoff = tripType.toLowerCase() === "dropoff";

  const validEmployees = employees.filter(
    (emp) =>
      emp.location &&
      typeof emp.location.lat === "number" &&
      typeof emp.location.lng === "number" &&
      !isNaN(emp.location.lat) &&
      !isNaN(emp.location.lng)
  );

  if (validEmployees.length === 0) {
    console.log("[processEmployeeBatch] No valid employees found.");
    return { routes: [] };
  }

  let globalRemainingEmployees = [...validEmployees].map((emp) => ({
    ...emp,
    distToFacility: haversineDistance(
      [emp.location.lat, emp.location.lng],
      [facility.geoY, facility.geoX]
    ),
    isMedical: emp.isMedical || false,
  }));

  globalRemainingEmployees.sort((a, b) =>
    isDropoff
      ? a.distToFacility - b.distToFacility
      : b.distToFacility - a.distToFacility
  );

//   console.log("processEmployeeBatch input employees with isMedical flags:");
// employees.forEach(emp => {
//   console.log(`empCode: ${emp.empCode}, isMedical: ${emp.isMedical}`);
// });


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

  while (globalRemainingEmployees.length > 0) {
    heuristicRouteCounter++;
    const originalPhysicalCapacity = maxCapacity;
    let routeIsCurrentlyMedical = false;
    let currentRouteMaxAllowedOccupancy = originalPhysicalCapacity;

    const firstEmployeeForThisRoute = globalRemainingEmployees.shift();
    if (!firstEmployeeForThisRoute) break;

    const currentAttemptRouteEmployees = [firstEmployeeForThisRoute];
    let tempRemainingEmployeesForThisAttempt = globalRemainingEmployees.slice();

    if (firstEmployeeForThisRoute.isMedical) {
      routeIsCurrentlyMedical = true;
      currentRouteMaxAllowedOccupancy = 2;
      console.log(
        `[Route ${heuristicRouteCounter}] Started route with medical employee ${firstEmployeeForThisRoute.empCode}. Max capacity set to 2.`
      );
    } else {
      console.log(
        `[Route ${heuristicRouteCounter}] Started route with non-medical employee ${firstEmployeeForThisRoute.empCode}. Capacity: ${originalPhysicalCapacity}`
      );
    }

    while (
      currentAttemptRouteEmployees.length < currentRouteMaxAllowedOccupancy &&
      tempRemainingEmployeesForThisAttempt.length > 0
    ) {
      const currentLastEmployeeInRoute =
        currentAttemptRouteEmployees[currentAttemptRouteEmployees.length - 1];
      const currentLoc = currentLastEmployeeInRoute.location;

      let scoredCandidates = tempRemainingEmployeesForThisAttempt
        .map((candidateEmp) => {
          if (
            candidateEmp.isMedical &&
            !routeIsCurrentlyMedical &&
            currentAttemptRouteEmployees.length >= 2
          ) {
            return null;
          }
          const candidateLoc = candidateEmp.location;
          const distanceToLastHaversine = haversineDistance(
            [currentLoc.lat, currentLoc.lng],
            [candidateLoc.lat, candidateLoc.lng]
          );
          if (distanceToLastHaversine > MAX_NEXT_STOP_DISTANCE_KM) return null;
          let progressScore = 0;
          if (isDropoff) {
            const d =
              candidateEmp.distToFacility -
              currentLastEmployeeInRoute.distToFacility;
            progressScore =
              d *
              PROGRESS_WEIGHT *
              (candidateEmp.distToFacility >=
              currentLastEmployeeInRoute.distToFacility *
                ACCEPTABLE_PROGRESS_FACTOR_DROPOFF
                ? 1
                : PROGRESS_PENALTY_SCALAR);
          } else {
            const d =
              currentLastEmployeeInRoute.distToFacility -
              candidateEmp.distToFacility;
            progressScore =
              d *
              PROGRESS_WEIGHT *
              (candidateEmp.distToFacility <
              currentLastEmployeeInRoute.distToFacility *
                ACCEPTABLE_PROGRESS_FACTOR_PICKUP
                ? 1
                : PROGRESS_PENALTY_SCALAR);
          }
          const distanceScoreVal =
            (1 / (1 + distanceToLastHaversine)) *
            DISTANCE_WEIGHT *
            DISTANCE_SCORE_SCALAR;
          return {
            emp: candidateEmp,
            score: progressScore + distanceScoreVal,
            distanceToLast: distanceToLastHaversine,
          };
        })
        .filter((item) => item != null && item.score > -Infinity);

      scoredCandidates.sort((a, b) => {
        if (Math.abs(b.score - a.score) > SCORE_DIFFERENCE_TOLERANCE)
          return b.score - a.score;
        return a.distanceToLast - b.distanceToLast;
      });

      if (scoredCandidates.length === 0) break;

      let nextEmployeeToPick = scoredCandidates[0]?.emp;
      if (!nextEmployeeToPick) break;

      console.log(
        `[Route ${heuristicRouteCounter}] Trying to add empCode ${nextEmployeeToPick.empCode} (isMedical: ${nextEmployeeToPick.isMedical}) to route with ${currentAttemptRouteEmployees.length} employees (medical route: ${routeIsCurrentlyMedical})`
      );

      if (
        !routeIsCurrentlyMedical &&
        nextEmployeeToPick.isMedical &&
        currentAttemptRouteEmployees.length >= 2
      ) {
        console.log(
          `[Route ${heuristicRouteCounter}] SKIPPING medical empCode ${nextEmployeeToPick.empCode} because route already has ${currentAttemptRouteEmployees.length} employees`
        );
        continue; // Skip adding this medical employee here
      }

      if (nextEmployeeToPick.isMedical && !routeIsCurrentlyMedical) {
        routeIsCurrentlyMedical = true;
        currentRouteMaxAllowedOccupancy = 2;
        console.log(
          `[Route ${heuristicRouteCounter}] Route converted to medical route due to empCode ${nextEmployeeToPick.empCode}`
        );
      }

      if (currentAttemptRouteEmployees.length >= currentRouteMaxAllowedOccupancy) {
        console.log(
          `[Route ${heuristicRouteCounter}] Route full with ${currentAttemptRouteEmployees.length} employees, stopping addition`
        );
        break;
      }

      currentAttemptRouteEmployees.push(nextEmployeeToPick);
      console.log(
        `[Route ${heuristicRouteCounter}] ADDED empCode ${nextEmployeeToPick.empCode}. Route now has ${currentAttemptRouteEmployees.length} employees`
      );

      const pickedIndex = tempRemainingEmployeesForThisAttempt.findIndex(
        (e) => e.empCode === nextEmployeeToPick.empCode
      );
      if (pickedIndex > -1)
        tempRemainingEmployeesForThisAttempt.splice(pickedIndex, 1);
    }

    console.log(
      `[Route ${heuristicRouteCounter}] FINAL route employees: ${currentAttemptRouteEmployees
        .map((e) => e.empCode)
        .join(", ")} (medical: ${routeIsCurrentlyMedical}, capacity limit: ${currentRouteMaxAllowedOccupancy})`
    );

    // --- GUARD and final route push logic ---

    let successfullyFormedRoute = false;
    if (currentAttemptRouteEmployees.length > 0) {
      const finalCapacityForOutputObject = routeIsCurrentlyMedical
        ? 2
        : originalPhysicalCapacity;

      if (currentAttemptRouteEmployees.length <= finalCapacityForOutputObject) {
        routes.push({
          employees: [...currentAttemptRouteEmployees],
          routeNumber: heuristicRouteCounter,
          vehicleCapacity: finalCapacityForOutputObject,
          zone: firstEmployeeForThisRoute.zone,
          tripType: isDropoff ? "dropoff" : "pickup",
          isMedicalRoute: routeIsCurrentlyMedical,
          uniqueKey: `${firstEmployeeForThisRoute.zone}_${heuristicRouteCounter}_${uuidv4()}`,
        });
        successfullyFormedRoute = true;
        globalRemainingEmployees = tempRemainingEmployeesForThisAttempt;
      }
    }

    if (!successfullyFormedRoute) {
      globalRemainingEmployees = tempRemainingEmployeesForThisAttempt;
    }
  }

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

async function generateRoutes(data) {
  try {
    const {
      employees,
      facility,
      shiftTime,
      date,
      profile,
      saveToDatabase = false,
      pickupTimePerEmployee = 180, // Default 3 mins
      reportingTime = 0,
      guard = false,
      tripType = "PICKUP",
    } = data;

    if (!employees?.length) throw new Error("Employee data is required");
    if (!facility?.geoX || !facility?.geoY)
      throw new Error("Valid facility data required");
    if (!date || !shiftTime || !profile)
      throw new Error("Missing required parameters");

    const osrmAvailable = await isOsrmAvailable();
    if (!osrmAvailable) throw new Error("OSRM routing service unavailable");

    const useZones =
      profile.zoneBasedRouting !== undefined
        ? !!profile.zoneBasedRouting
        : true;
    let employeesByZone = {};
    const removedForGuardByZone = {};

    if (useZones) {
      let zones = data.zones || [];
      if (!zones.length && ZONES_DATA_FILE) {
        try {
          zones = await loadZonesData();
          if (!zones.length) console.warn("No zones data loaded.");
        } catch (err) {
          console.error(`Failed to load zones: ${err.message}.`);
        }
      }
      employeesByZone = assignEmployeesToZones(employees, zones);
      if (Object.keys(employeesByZone).length === 0 && employees.length > 0) {
        if (!employeesByZone["DEFAULT_ZONE"])
          employeesByZone["DEFAULT_ZONE"] = [];
        employees.forEach((emp) => {
          // Ensure all employees are in some zone
          if (
            !Object.values(employeesByZone)
              .flat()
              .find((e) => e.empCode === emp.empCode)
          ) {
            employeesByZone["DEFAULT_ZONE"].push({
              ...emp,
              zone: "DEFAULT_ZONE",
              location: { lat: emp.geoY, lng: emp.geoX },
            });
          }
        });
        if (employeesByZone["DEFAULT_ZONE"].length > 0)
          console.log("Some employees assigned to DEFAULT_ZONE.");
      }
    } else {
      employeesByZone = {
        GLOBAL: employees.map((emp) => ({
          ...emp,
          zone: "GLOBAL",
          location: { lat: emp.geoY, lng: emp.geoX },
        })),
      };
    }

    const routeData = {
      uuid: data.uuid || uuidv4(),
      date,
      shift: shiftTime,
      tripType: tripType.toUpperCase(),
      facility,
      profile,
      employeeData: employees,
      routeData: [],
    };

    const processedZones = new Set();
    const { zonePairingMatrix = {}, maxDuration: profileMaxDuration = 7200 } =
      profile;
    let totalRouteCount = 0;
    let finalTotalSwappedRoutes = 0;
    const allInitiallyFormedRoutes = []; // Routes from heuristic
    let unroutedByOrTools = []; // Employees OR-Tools couldn't route

    const isDropoff = tripType.toLowerCase() === "dropoff";

    // --- Stage 1: Use processEmployeeBatch to form initial routes (employee groups) ---
    const processZoneOrGroup = async (
      empsInScope,
      zoneIdentifier,
      effectiveMaxCapacity
    ) => {
      if (empsInScope.length === 0) return;
      console.log(
        `[Heuristic Stage] Forming initial routes for "${zoneIdentifier}" with ${empsInScope.length} employees. MaxCap: ${effectiveMaxCapacity}`
      );
      const { routes: batchRoutes } = await processEmployeeBatch(
        empsInScope,
        effectiveMaxCapacity,
        facility,
        tripType,
        profileMaxDuration,
        pickupTimePerEmployee,
        guard
      );
      batchRoutes.forEach((route) => {
        route.zone = zoneIdentifier; // Ensure zone is set correctly
        allInitiallyFormedRoutes.push(route);
      });
    };

    if (profile.zoneClubbing) {
      const zoneGroups = findZoneGroups(zonePairingMatrix);
      for (const group of zoneGroups) {
        const clubbedZoneName = group.join("-");
        const combinedEmployees = group
          .flatMap((zn) => employeesByZone[zn] || [])
          .filter((e) => e.location);
        const maxCap = Math.max(
          ...group.map((z) => getZoneCapacity(z, profile)),
          1
        );
        await processZoneOrGroup(combinedEmployees, clubbedZoneName, maxCap);
        group.forEach((z) => processedZones.add(z));
      }
    }

    for (const [zoneName, zoneEmpList] of Object.entries(employeesByZone)) {
      if (processedZones.has(zoneName)) continue;
      const currentZoneEmployees = (zoneEmpList || []).filter(
        (e) => e.location
      );
      const maxCap = getZoneCapacity(zoneName, profile);
      await processZoneOrGroup(currentZoneEmployees, zoneName, maxCap);
    }

    console.log(
      `[Heuristic Stage] Total initial routes formed: ${allInitiallyFormedRoutes.length}`
    );

    // --- Stage 2: Optimize sequence of each heuristically formed route using OR-Tools ---
    const allOptimizedOrToolsRoutes = [];
    console.log(
      `\n[OR-Tools Stage] Optimizing sequence for ${allInitiallyFormedRoutes.length} heuristically formed routes.`
    );

    for (const initialRoute of allInitiallyFormedRoutes) {
      if (!initialRoute.employees || initialRoute.employees.length === 0) {
        console.warn(
          `  Skipping OR-Tools for empty heuristic route in zone ${initialRoute.zone}.`
        );
        continue;
      }
      console.log(
        `  Optimizing route for zone ${initialRoute.zone} with ${initialRoute.employees.length} employees (original heuristic order).`
      );
      try {
        const { routes: orToolsSolvedRouteList, droppedEmployees } =
          await solveZoneWithORTools(
            initialRoute.employees,
            facility,
            initialRoute.vehicleCapacity,
            profileMaxDuration,
            pickupTimePerEmployee,
            tripType,
            initialRoute.zone,
            true // forceSingleVehicleOptimization = true
          );

        if (droppedEmployees && droppedEmployees.length > 0) {
          console.warn(
            `  [OR-Tools Stage] Zone ${initialRoute.zone} - OR-Tools dropped ${
              droppedEmployees.length
            } employees while optimizing sequence: ${droppedEmployees
              .map((e) => e.empCode)
              .join(",")}. Adding them to unrouted list.`
          );
          unroutedByOrTools.push(...droppedEmployees);
        }

        if (orToolsSolvedRouteList && orToolsSolvedRouteList.length > 0) {
          // Assuming solveZoneWithORTools (with forceSingleVehicleOptimization=true) returns at most one route for the input employees
          const optimizedRouteSegment = orToolsSolvedRouteList[0];
          const finalRoute = {
            ...initialRoute, // Preserve original zone, initial guardNeeded, initial vehicleCapacity
            employees: optimizedRouteSegment.employees, // Now ordered by OR-Tools
          };
          allOptimizedOrToolsRoutes.push(finalRoute);
          if (orToolsSolvedRouteList.length > 1) {
            console.warn(
              `  [OR-TOOLS Stage] Zone ${initialRoute.zone} - OR-Tools split a pre-formed heuristic route. This is unexpected with forceSingleVehicleOptimization=true.`
            );
            // Handle additional segments if any - for now, we only take the first.
          }
        } else {
          console.warn(
            `  [OR-Tools Stage] OR-Tools found no solution for pre-formed route in zone ${initialRoute.zone}. Using original heuristic order for this route.`
          );
          allOptimizedOrToolsRoutes.push(initialRoute);
        }
      } catch (error) {
        console.error(
          `  [OR-Tools Stage] Error optimizing route for zone ${initialRoute.zone}: ${error.message}. Using original heuristic order.`
        );
        allOptimizedOrToolsRoutes.push(initialRoute);
      }
    }

    // --- Post-Processing Loop (on OR-Tools re-sequenced or fallback heuristic routes) ---
    const finalProcessedRoutes = [];
    console.log(
      `\nStarting post-processing for ${allOptimizedOrToolsRoutes.length} routes.`
    );
    for (const route of allOptimizedOrToolsRoutes) {
      try {
        totalRouteCount++;
        route.routeNumber = totalRouteCount;
        let routeModifiedByGuardSwap = false;

        if (!route?.employees?.length) {
          assignErrorState(
            route,
            "Route has no employees before final OSRM call"
          );
          finalProcessedRoutes.push(route);
          continue;
        }

        const routeCoordinates = route.employees.map((emp) => [
          emp.location.lat,
          emp.location.lng,
        ]);
        const facilityCoordinates = [facility.geoY, facility.geoX];
        const currentAllCoordinates = isDropoff
          ? [facilityCoordinates, ...routeCoordinates]
          : [...routeCoordinates, facilityCoordinates];

        let currentRouteDetails = await calculateRouteDetails(
          currentAllCoordinates,
          route.employees,
          pickupTimePerEmployee,
          tripType
        );

        if (currentRouteDetails.error) {
          assignErrorState(
            route,
            `OSRM /trip failed for final route: ${currentRouteDetails.error}`
          );
          finalProcessedRoutes.push(route);
          continue;
        }
        updateRouteWithDetails(route, currentRouteDetails);

        if (guard && route.employees.length > 0) {
          const guardSwapResult = await handleGuardRequirements(
            route,
            isDropoff,
            facility,
            pickupTimePerEmployee
          );
          if (
            guardSwapResult.swapped &&
            guardSwapResult.routeDetails &&
            !guardSwapResult.routeDetails.error
          ) {
            routeModifiedByGuardSwap = true;
            finalTotalSwappedRoutes++;
            route.guardNeeded = false; // Guard avoided by swap
            // The route.employees are now in the swapped order from guardSwapResult.routeDetails.employees
            // And routeDetails (including geometry) are also from guardSwapResult.routeDetails
            updateRouteWithDetails(route, guardSwapResult.routeDetails);

            // <<< --- NEW: Attempt Re-optimization --- >>>
            console.log(
              `Route ${route.routeNumber} was swapped. Attempting OR-Tools re-optimization with fixed guard.`
            );
            const reOptResult = await reOptimizeSwappedRouteWithORTools(
              { ...route }, // Pass a copy of the current route state
              facility,
              pickupTimePerEmployee
            );

            if (reOptResult.reOptimized && reOptResult.employees.length > 0) {
              console.log(
                `Route ${route.routeNumber} successfully re-optimized by OR-Tools after swap.`
              );
              route.employees = reOptResult.employees; // Update with the new OR-Tools sequence

              // IMPORTANT: After OR-Tools re-optimization, the geometry and precise timings
              // from OSRM /trip are needed again for this *new* sequence.
              const reOptRouteCoordinates = route.employees.map((emp) => [
                emp.location.lat,
                emp.location.lng,
              ]);
              const reOptAllCoordinates = isDropoff
                ? [facilityCoordinates, ...reOptRouteCoordinates]
                : [...reOptRouteCoordinates, facilityCoordinates];

              currentRouteDetails = await calculateRouteDetails(
                // Recalculate with OSRM /trip
                reOptAllCoordinates,
                route.employees,
                pickupTimePerEmployee,
                tripType
              );

              if (currentRouteDetails.error) {
                console.warn(
                  `OSRM /trip failed for re-optimized route ${route.routeNumber}: ${currentRouteDetails.error}. Falling back to pre-reOpt details.`
                );
                // If OSRM fails after re-opt, we might revert or handle error.
                // For now, the route.employees are updated, but details might be stale if this fails.
                // Or, better, revert employees too if OSRM fails for the new sequence.
                // For simplicity here, we'll proceed, but in production, you'd want robust fallback.
                assignErrorState(
                  route,
                  `OSRM /trip failed after re-optimization: ${currentRouteDetails.error}`
                );
              } else {
                updateRouteWithDetails(route, currentRouteDetails); // Update with final details
              }
            } else {
              console.log(
                `Route ${route.routeNumber} re-optimization after swap failed or yielded no improvement. Using OSRM details from initial swap. Error: ${reOptResult.error}`
              );
              // No change to route.employees or currentRouteDetails if re-opt failed;
              // they are already set from the initial successful swap.
            }
            // <<< --- END NEW --- >>>
          } else if (guardSwapResult.guardNeeded) {
            route.guardNeeded = true;
            let originalVehicleCapacity = route.vehicleCapacity;
            route.vehicleCapacity = Math.max(
              1,
              originalVehicleCapacity - (route.guardNeeded ? 1 : 0)
            );
            console.log(
              `Route ${route.routeNumber} needs guard. Capacity reduced from ${originalVehicleCapacity} to ${route.vehicleCapacity}.`
            );

            if (route.employees.length > route.vehicleCapacity) {
              const removedEmp = route.employees.pop();
              if (removedEmp) {
                console.log(
                  `  Guard forced removal of ${removedEmp.empCode} from route ${route.routeNumber}.`
                );
                if (!removedForGuardByZone[route.zone])
                  removedForGuardByZone[route.zone] = [];
                removedForGuardByZone[route.zone].push(removedEmp);
                if (route.employees.length > 0) {
                  const newCoords = route.employees.map((e) => [
                    e.location.lat,
                    e.location.lng,
                  ]);
                  const newAllCoords = isDropoff
                    ? [facilityCoordinates, ...newCoords]
                    : [...newCoords, facilityCoordinates];
                  const recalcDetails = await calculateRouteDetails(
                    newAllCoords,
                    route.employees,
                    pickupTimePerEmployee,
                    tripType
                  );
                  if (!recalcDetails.error)
                    updateRouteWithDetails(route, recalcDetails);
                  else
                    assignErrorState(
                      route,
                      `OSRM failed after guard removal: ${recalcDetails.error}`
                    );
                } else {
                  assignErrorState(route, "No employees after guard removal");
                }
              }
            }
          }
        }

        if (route.employees.length > 0 && !route.error) {
          calculatePickupTimes(
            route,
            shiftTime,
            pickupTimePerEmployee,
            reportingTime
          );
          if (
            profileMaxDuration &&
            route.routeDetails &&
            route.routeDetails.duration > profileMaxDuration
          ) {
            route.durationExceeded = true;
            console.warn(`Route ${route.routeNumber} exceeds max duration.`);
          }
        } else if (!route.error) {
          assignErrorState(
            route,
            "Route became empty/errored before time calc"
          );
        }
        route.swapped = routeModifiedByGuardSwap;
        finalProcessedRoutes.push(route);
      } catch (error) {
        console.error(
          `Critical error in post-processing loop for route ${route?.routeNumber}:`,
          error
        );
        if (route) {
          assignErrorState(
            route,
            `Post-processing loop error: ${error.message}`
          );
          finalProcessedRoutes.push(route);
        }
      }
    }
    routeData.routeData = finalProcessedRoutes; // Assign the fully processed routes

    // Handle employees unrouted by OR-Tools or removed for guard
    let finalUnroutedEmployees = [...unroutedByOrTools];
    for (const empList of Object.values(removedForGuardByZone)) {
      finalUnroutedEmployees.push(...empList);
    }
    // Deduplicate finalUnroutedEmployees by empCode
    finalUnroutedEmployees = Array.from(
      new Map(finalUnroutedEmployees.map((emp) => [emp.empCode, emp])).values()
    );

    if (finalUnroutedEmployees.length > 0) {
      console.log(
        `\n[Re-insertion Stage for OR-Tools/Guard Unrouted] Attempting to re-insert ${finalUnroutedEmployees.length} employees.`
      );
      // Simplified: Create new routes for these using processEmployeeBatch (could also try OR-Tools again)
      const { routes: newRoutesForUnrouted } = await processEmployeeBatch(
        finalUnroutedEmployees,
        getZoneCapacity("DEFAULT_ZONE", profile), // Use a default capacity
        facility,
        tripType,
        profileMaxDuration,
        pickupTimePerEmployee,
        guard
      );

      // for (const newRoute of newRoutesForUnrouted) {
      //     totalRouteCount++;
      //     newRoute.routeNumber = totalRouteCount;
      //     newRoute.zone = newRoute.zone || "UNROUTED_REINSERTED"; // Assign a zone
      //      if (newRoute.employees.length > 0) {
      //         const newRouteCoords = newRoute.employees.map((e) => [e.location.lat, e.location.lng]);
      //         const facilityCoords = [facility.geoY, facility.geoX];
      //         const newAllCoords = isDropoff ? [facilityCoords, ...newRouteCoords] : [...newRouteCoords, facilityCoords];
      //         const finalDetails = await calculateRouteDetails(newAllCoords, newRoute.employees, pickupTimePerEmployee, tripType);
      //         if (!finalDetails.error) {
      //             updateRouteWithDetails(newRoute, finalDetails);
      //             calculatePickupTimes(newRoute, shiftTime, pickupTimePerEmployee, reportingTime);
      //         } else {
      //             assignErrorState(newRoute, `OSRM failed for re-inserted unrouted: ${finalDetails.error}`);
      //         }
      //     } else {
      //         assignErrorState(newRoute, "Re-inserted unrouted route has no employees");
      //     }
      //     routeData.routeData.push(newRoute);
      // }
    }

    const stats = calculateRouteStatistics(routeData, employees.length);
    const response = createSimplifiedResponse({
      ...routeData,
      ...stats,
      totalSwappedRoutes: finalTotalSwappedRoutes,
    });

    if (saveToDatabase) {
      console.log("Simulating save to database for UUID:", response.uuid);
    }
    return response;
  } catch (error) {
    console.error("Top-level generateRoutes error:", error);
    const inputData = typeof data === "object" && data !== null ? data : {};
    return createEmptyResponse({
      uuid: inputData.uuid,
      date: inputData.date,
      shiftTime: inputData.shiftTime,
      tripType: inputData.tripType,
      employees: inputData.employees,
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
  facility,
  pickupTimePerEmployee
) {
  try {
    if (!route?.employees?.length) {
      return { guardNeeded: false, swapped: false };
    }

    const checkIndex = isDropoff ? route.employees.length - 1 : 0;
    const criticalEmployee = route.employees[checkIndex];

    if (!criticalEmployee || criticalEmployee.gender !== "F") {
      return { guardNeeded: false, swapped: false };
    }

    const swapCandidates = route.employees
      .map(async (emp, index) => {
        if (index === checkIndex || emp.gender !== "M") {
          return null;
        }
        const distance = await calculateDistance(
          // Haversine for candidate search
          [criticalEmployee.location.lat, criticalEmployee.location.lng],
          [emp.location.lat, emp.location.lng]
        );
        return { employee: emp, index, distance };
      })
      .filter(Boolean);

    const validCandidates = (await Promise.all(swapCandidates))
      .filter(
        (candidate) =>
          candidate &&
          candidate.distance !== Infinity &&
          candidate.distance <= MAX_SWAP_DISTANCE_KM
      )
      .sort((a, b) => a.distance - b.distance);

    if (validCandidates.length === 0) {
      console.log(
        `Guard needed for route ${route.routeNumber}: No suitable swap candidates found within ${MAX_SWAP_DISTANCE_KM}km.`
      );
      return { guardNeeded: true, swapped: false };
    }

    const bestCandidate = validCandidates[0];
    const newEmployees = [...route.employees];
    [newEmployees[checkIndex], newEmployees[bestCandidate.index]] = [
      newEmployees[bestCandidate.index],
      newEmployees[checkIndex],
    ];

    const newCoordinates = newEmployees.map((emp) => [
      emp.location.lat,
      emp.location.lng,
    ]);
    const facilityCoordinates = [facility.geoY, facility.geoX];
    const allCoordinates = isDropoff
      ? [facilityCoordinates, ...newCoordinates]
      : [...newCoordinates, facilityCoordinates];

    const routeDetailsAfterSwap = await calculateRouteDetails(
      // Recalculate with OSRM
      allCoordinates,
      newEmployees, // Pass the potentially swapped employee list
      pickupTimePerEmployee,
      route.tripType
    );

    if (routeDetailsAfterSwap.error) {
      console.warn(
        `Swap validation failed for route ${route.routeNumber} due to OSRM error: ${routeDetailsAfterSwap.error}`
      );
      return { guardNeeded: true, swapped: false }; // Revert or mark guard needed
    }

    // Update route with swapped employees and new details
    // route.employees = routeDetailsAfterSwap.employees; // calculateRouteDetails now returns ordered employees
    // updateRouteWithDetails(route, routeDetailsAfterSwap); // This will be done in the main loop

    console.log(
      `Successfully swapped employees in route ${
        route.routeNumber
      } for guard: ${criticalEmployee.empCode} with ${
        bestCandidate.employee.empCode
      }. Distance: ${bestCandidate.distance.toFixed(2)}km`
    );
    return {
      guardNeeded: false,
      swapped: true,
      // newCoordinates: allCoordinates, // Not strictly needed by caller if routeDetails is returned
      routeDetails: routeDetailsAfterSwap, // Return the new details
    };
  } catch (error) {
    console.error(
      `Error in handleGuardRequirements for route ${route?.routeNumber}:`,
      error
    );
    return { guardNeeded: true, swapped: false };
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

function createSimplifiedResponse(routeData) {
  return {
    uuid: routeData.uuid,
    date: routeData.date,
    shift: routeData.shift,
    tripType: routeData.tripType === "PICKUP" ? "P" : "D",
    totalEmployees: routeData.totalEmployees,
    totalRoutedEmployees: routeData.totalRoutedEmployees,
    totalRoutes: routeData.totalRoutes,
    averageOccupancy: routeData.averageOccupancy,
    overallRouteDetails: routeData.routeDetails,
    totalSwappedRoutes: routeData.totalSwappedRoutes,
    routes: routeData.routeData
      .filter((route) => !route.error && route.employees?.length > 0)
      .map((route) => {
        const guardPresent =
          route.guardNeeded ||
          (route.guard && route.employees.some((e) => e.isGuard));
        const occupancy =
          (route.employees?.length || 0) +
          (guardPresent && !route.employees.some((e) => e.isGuard) ? 1 : 0);

        return {
          routeNumber: route.routeNumber,
          zone: route.zone,
          vehicleCapacity: route.vehicleCapacity,
          guard: guardPresent,
          swapped: route.swapped || false,
          durationExceeded: route.durationExceeded || false,
          uniqueKey: route.uniqueKey,
          isMedicalRoute: route.isMedicalRoute,
          distance: parseFloat(
            ((route.routeDetails?.distance || 0) / 1000).toFixed(2)
          ),
          duration: parseFloat((route.routeDetails?.duration || 0).toFixed(2)),
          occupancy,
          encodedPolyline: route.encodedPolyline || "no_polyline",
          employees: (route.employees || []).map((emp, index) => ({
            empCode: emp.empCode,
            gender: emp.gender,
            eta:
              route.tripType?.toUpperCase() === "DROPOFF"
                ? emp.dropoffTime
                : emp.pickupTime,
            order:
              emp.order !== undefined && emp.order >= 1 ? emp.order : index + 1,
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
