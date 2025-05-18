const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const { spawn } = require("child_process"); // For calling Python

const TRAFFIC_BUFFER_PERCENTAGE = 0.4; // 40% buffer for traffic
const MAX_SWAP_DISTANCE_KM = 1.5; // or your business threshold

// Constants for processEmployeeBatch (your original heuristic)
const OSRM_PROBE_COUNT_HEURISTIC = 5; // Can be different from OR-Tools related OSRM_PROBE_COUNT
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
    return false;
  }

  const st = parseInt(shiftTime.toString(), 10);
  if (isNaN(st)) {
    return false;
  }

  let nightShiftConfig = profile?.nightShiftGuardTimings;
  if (!nightShiftConfig) {
    nightShiftConfig = {
      PICKUP: { start: 2000, end: 700 },
      DROPOFF: { start: 1900, end: 530 },
    };
  }

  const facilityTypePrefix = (profile?.facilityType || "CDC").toUpperCase();
  const typeConfigKeyWithFacility = `${facilityTypePrefix}_${tripType.toUpperCase()}`;
  let config = nightShiftConfig[typeConfigKeyWithFacility];

  if (!config) {
    config = nightShiftConfig[tripType.toUpperCase()];
  }

  if (!config) {
    return false;
  }

  const { start, end } = config;

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
  if (!emp) return false;
  return (emp.isMedical || false) || (emp.isPWD || false);
};

function checkRouteDeviation(route, facility, profile) {
  // console.log(`[Route ${route.routeNumber}] checkRouteDeviation called.`);
  if (!profile) {
    // console.warn(`[Route ${route.routeNumber}] No profile object provided to checkRouteDeviation. Returning true.`);
    return true;
  }
  if (!profile.routeDeviationRules) {
    // console.warn(`[Route ${route.routeNumber}] profile.routeDeviationRules is missing. Returning true.`);
    return true;
  }
  if (
    !route?.routeDetails?.totalDistance || // Check for totalDistance
    !route?.employees?.length
  ) {
    // console.warn(`[Route ${route.routeNumber}] Missing routeDetails, totalDistance, or employees. Returning true.`);
    return true;
  }

  const facilityType = profile.facilityType || "CDC";
  // console.log(`[Route ${route.routeNumber}] Using facilityType: ${facilityType}`);
  const rules =
    profile.routeDeviationRules[facilityType] ||
    profile.routeDeviationRules["DEFAULT"];

  if (!rules || rules.length === 0) {
    // console.warn(`[Route ${route.routeNumber}] No deviation rules found for facilityType: ${facilityType}. Returning true.`);
    return true;
  }

  let maxHaversineDistKm = 0;
  for (const emp of route.employees) {
    if (emp.location) {
      const dist = haversineDistance(
        [facility.geoY, facility.geoX],
        [emp.location.lat, emp.location.lng]
      );
      if (dist > maxHaversineDistKm) {
        maxHaversineDistKm = dist;
      }
    }
  }
  // console.log(`[Route ${route.routeNumber}] Max Haversine distance to furthest employee: ${maxHaversineDistKm.toFixed(2)} km`);

  if (maxHaversineDistKm === 0 && route.employees.length > 0) {
    // console.warn(`[Route ${route.routeNumber}] maxHaversineDistKm is 0 with employees present. Returning true (cannot determine rule).`);
    return true;
  }

  let applicableRule = rules.find(
    (rule) =>
      maxHaversineDistKm >= rule.minDistKm &&
      maxHaversineDistKm <= rule.maxDistKm
  );

  if (!applicableRule) {
    const sortedRules = [...rules].sort((a, b) => a.maxDistKm - b.maxDistKm);
    const lastRule = sortedRules[sortedRules.length - 1];
    if (lastRule && maxHaversineDistKm > lastRule.maxDistKm) {
      // console.log(`[Route ${route.routeNumber}] Furthest emp ${maxHaversineDistKm.toFixed(2)}km is beyond last rule's max (${lastRule.maxDistKm}km). Applying last rule's limit.`);
      applicableRule = lastRule;
    } else {
      // console.warn(`[Route ${route.routeNumber}] No specific deviation rule for furthest distance ${maxHaversineDistKm.toFixed(2)}km. Assuming okay.`);
      return true;
    }
  }

  if (!applicableRule || applicableRule.maxTotalOneWayKm == null) {
    // console.error(`[Route ${route.routeNumber}] Could not determine applicable rule or rule is malformed (missing maxTotalOneWayKm). Furthest: ${maxHaversineDistKm.toFixed(2)}km. Assuming okay to prevent error.`);
    return true;
  }

  // console.log(`[Route ${route.routeNumber}] Applicable rule: min ${applicableRule.minDistKm}km, max ${applicableRule.maxDistKm}km, maxTotalOneWay ${applicableRule.maxTotalOneWayKm}km`);

  const actualOneWayKm = route.routeDetails.totalDistance / 1000; // Use totalDistance
  const maxAllowedKm = applicableRule.maxTotalOneWayKm;

  // console.log(`[Route ${route.routeNumber}] Actual OSRM one-way: ${actualOneWayKm.toFixed(2)} km. Max allowed by rule: ${maxAllowedKm.toFixed(2)} km.`);

  if (actualOneWayKm > maxAllowedKm) {
    // console.warn(`  [Route ${route.routeNumber}] DEVIATION EXCEEDED. Allowed: ${maxAllowedKm.toFixed(2)}km, Actual: ${actualOneWayKm.toFixed(2)}km. Returning false.`);
    return false;
  }

  // console.log(`  [Route ${route.routeNumber}] Deviation OK. Returning true.`);
  return true;
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

async function reOptimizeSwappedRouteWithORTools(
  routeToReOptimize,
  facilityData,
  pickupTimePerEmployee
) {
  const {
    employees: swappedEmployees,
    tripType,
    zone,
    vehicleCapacity,
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

  if (tripType.toLowerCase() === "pickup") {
    pinnedEmployee = swappedEmployees[0];
    otherEmployeesInRoute = swappedEmployees.slice(1);
  } else {
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

  const employeesForThisOrRun = [pinnedEmployee, ...otherEmployeesInRoute];
  const pointMapForReSolve = [
    { empCode: "FACILITY", isFacility: true, ...facilityLocation },
    ...employeesForThisOrRun.map((emp) => ({ ...emp })),
  ];

  const pinnedNodeIndexInMatrix = 1;
  const otherCustomerIndicesInMatrix = otherEmployeesInRoute.map(
    (_, i) => i + 2
  );

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
      employeesForThisOrRun,
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

    const demands = [0, ...employeesForThisOrRun.map(() => 1)];
    const serviceTimes = [
      0,
      ...employeesForThisOrRun.map(() => pickupTimePerEmployee),
    ];

    const orToolsInput = {
      distance_matrix: distanceMatrix,
      duration_matrix: durationMatrix,
      num_vehicles: 1,
      vehicle_capacities: [vehicleCapacity],
      demands: demands,
      depot_index: 0,
      max_route_duration: profileMaxDuration,
      service_times: serviceTimes,
      allow_dropping_visits: false,
      facility_coords: [facilityLocation.lat, facilityLocation.lng],
      trip_type: tripType.toUpperCase(),
      direction_penalty_weight:
        facilityData.profile?.directionPenaltyWeight || 0.5,
      ...fixedNodeParam,
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
        let solution = null;
        let parsedSuccessfully = false;
        try {
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
          const routeNodeIndices = solution.routes[0];
          const reOptimizedEmployeeList = routeNodeIndices
            .map((nodeIndex) => {
              if (nodeIndex === 0) return null;
              if (nodeIndex >= pointMapForReSolve.length) {
                console.error(
                  `[RE-OPTIMIZE] Route nodeIndex ${nodeIndex} out of bounds for pointMap length ${pointMapForReSolve.length}`
                );
                return null;
              }
              return pointMapForReSolve[nodeIndex];
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

async function processEmployeeBatch(
  employees,
  maxCapacity,
  facility,
  tripType = "pickup",
  maxDuration,
  pickupTimePerEmployee,
  guard = false
) {
  const routes = [];
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const facilityCoordinates = [facility.geoY, facility.geoX];
  const deferredEmployees = []; // Track employees that couldn't be added to current route

  const validEmployees = employees.filter(
    (emp) =>
      emp.location &&
      typeof emp.location.lat === "number" &&
      typeof emp.location.lng === "number" &&
      !isNaN(emp.location.lat) &&
      !isNaN(emp.location.lng)
  );

  if (validEmployees.length === 0) {
    return { routes: [] };
  }

  const isSpecialNeedsUser = (emp) =>
    (emp.isMedical || false) || (emp.isPWD || false);

  let globalRemainingEmployees = [...validEmployees].map((emp) => ({
    ...emp,
    distToFacility: haversineDistance(
      [emp.location.lat, emp.location.lng],
      [facility.geoY, facility.geoX]
    ),
    isMedical: emp.isMedical || false,
    isPWD: emp.isPWD || false,
  }));

  globalRemainingEmployees.sort((a, b) =>
    isDropoff
      ? a.distToFacility - b.distToFacility
      : b.distToFacility - a.distToFacility
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

    // Validate first employee with OSRM
    const firstEmpCoords = [firstEmployeeForThisRoute.location.lat, firstEmployeeForThisRoute.location.lng];
    const firstRouteCoords = isDropoff 
      ? [facilityCoordinates, firstEmpCoords]
      : [firstEmpCoords, facilityCoordinates];
    
    const firstRouteDetails = await calculateRouteDetails(
      firstRouteCoords,
      [firstEmployeeForThisRoute],
      pickupTimePerEmployee,
      tripType
    );

    if (firstRouteDetails.error) {
      console.warn(`First employee ${firstEmployeeForThisRoute.empCode} failed OSRM validation. Adding to deferred.`);
      deferredEmployees.push(firstEmployeeForThisRoute);
      continue;
    }

    // Create initial route object with OSRM details
    const currentRoute = {
      employees: [firstEmployeeForThisRoute],
      routeNumber: heuristicRouteCounter,
      vehicleCapacity: originalPhysicalCapacity,
      guardNeeded: false,
      uniqueKey: `${firstEmployeeForThisRoute.zone}_${heuristicRouteCounter}_${uuidv4()}`,
      zone: firstEmployeeForThisRoute.zone,
      tripType: isDropoff ? "dropoff" : "pickup",
      isSpecialNeedsRoute: isSpecialNeedsUser(firstEmployeeForThisRoute),
      routeDetails: firstRouteDetails
    };

    // Check deviation and duration for first employee
    const deviationOkay = checkRouteDeviation(currentRoute, facility, facility.profile);
    if (!deviationOkay) {
      console.warn(`First employee ${firstEmployeeForThisRoute.empCode} failed deviation check. Adding to deferred.`);
      deferredEmployees.push(firstEmployeeForThisRoute);
      continue;
    }

    if (maxDuration && firstRouteDetails.totalDuration > maxDuration) {
      console.warn(`First employee ${firstEmployeeForThisRoute.empCode} exceeds max duration. Adding to deferred.`);
      deferredEmployees.push(firstEmployeeForThisRoute);
      continue;
    }

    if (isSpecialNeedsUser(firstEmployeeForThisRoute)) {
      routeIsCurrentlySpecialNeeds = true;
      currentRouteMaxAllowedOccupancy = 2;
    }

    let tempRemainingEmployeesForThisAttempt = globalRemainingEmployees.map(
      (e) => ({ ...e })
    );

    while (
      currentRoute.employees.length < currentRouteMaxAllowedOccupancy &&
      tempRemainingEmployeesForThisAttempt.length > 0
    ) {
      const currentLastEmployeeInRoute = currentRoute.employees[currentRoute.employees.length - 1];
      const currentLoc = currentLastEmployeeInRoute.location;

      let scoredCandidates = tempRemainingEmployeesForThisAttempt
        .map((candidateEmp, candidateIdx) => {
          const candidateIsSpecial = isSpecialNeedsUser(candidateEmp);

          if (routeIsCurrentlySpecialNeeds) {
            if (!candidateIsSpecial) return null;
          } else {
            if (candidateIsSpecial) {
              if (currentRoute.employees.length > 0 && !isSpecialNeedsUser(currentRoute.employees[0])) {
                return null;
              }
            }
          }

          const distanceToLastHaversine = haversineDistance(
            [currentLoc.lat, currentLoc.lng],
            [candidateEmp.location.lat, candidateEmp.location.lng]
          );
          if (distanceToLastHaversine > MAX_NEXT_STOP_DISTANCE_KM) return null;

          let progressScore = 0;
          if (isDropoff) {
            const d = candidateEmp.distToFacility - currentLastEmployeeInRoute.distToFacility;
            progressScore = d * PROGRESS_WEIGHT * (
              candidateEmp.distToFacility >= currentLastEmployeeInRoute.distToFacility * ACCEPTABLE_PROGRESS_FACTOR_DROPOFF
                ? 1
                : PROGRESS_PENALTY_SCALAR
            );
          } else {
            const d = currentLastEmployeeInRoute.distToFacility - candidateEmp.distToFacility;
            progressScore = d * PROGRESS_WEIGHT * (
              candidateEmp.distToFacility < currentLastEmployeeInRoute.distToFacility * ACCEPTABLE_PROGRESS_FACTOR_PICKUP
                ? 1
                : PROGRESS_PENALTY_SCALAR
            );
          }
          const distanceScoreVal = (1 / (1 + distanceToLastHaversine)) * DISTANCE_WEIGHT * DISTANCE_SCORE_SCALAR;
          return {
            emp: candidateEmp,
            score: progressScore + distanceScoreVal,
            distanceToLast: distanceToLastHaversine,
            originalIndex: candidateIdx,
          };
        })
        .filter((item) => item != null && item.score > -Infinity);

      scoredCandidates.sort((a, b) => {
        if (Math.abs(b.score - a.score) > SCORE_DIFFERENCE_TOLERANCE)
          return b.score - a.score;
        return a.distanceToLast - b.distanceToLast;
      });

      if (scoredCandidates.length === 0) break;

      let nextEmployeeToPickData = scoredCandidates[0];
      let nextEmployeeToPick = nextEmployeeToPickData?.emp;

      if (!nextEmployeeToPick) break;

      // Try adding the candidate and validate with OSRM
      const tentativeRoute = {
        ...currentRoute,
        employees: [...currentRoute.employees, nextEmployeeToPick]
      };

      const tentativeCoords = tentativeRoute.employees.map(emp => [emp.location.lat, emp.location.lng]);
      const allCoords = isDropoff 
        ? [facilityCoordinates, ...tentativeCoords]
        : [...tentativeCoords, facilityCoordinates];

      const tentativeDetails = await calculateRouteDetails(
        allCoords,
        tentativeRoute.employees,
        pickupTimePerEmployee,
        tripType
      );

      if (tentativeDetails.error) {
        console.warn(`Candidate ${nextEmployeeToPick.empCode} failed OSRM validation. Skipping.`);
        tempRemainingEmployeesForThisAttempt.splice(nextEmployeeToPickData.originalIndex, 1);
        continue;
      }

      // Check deviation and duration for tentative route
      const tentativeDeviationOkay = checkRouteDeviation(tentativeRoute, facility, facility.profile);
      if (!tentativeDeviationOkay) {
        console.warn(`Candidate ${nextEmployeeToPick.empCode} failed deviation check. Skipping.`);
        tempRemainingEmployeesForThisAttempt.splice(nextEmployeeToPickData.originalIndex, 1);
        continue;
      }

      if (maxDuration && tentativeDetails.totalDuration > maxDuration) {
        console.warn(`Candidate ${nextEmployeeToPick.empCode} exceeds max duration. Skipping.`);
        tempRemainingEmployeesForThisAttempt.splice(nextEmployeeToPickData.originalIndex, 1);
        continue;
      }

      // Candidate passed all checks - add to route
      currentRoute.employees.push(nextEmployeeToPick);
      currentRoute.routeDetails = tentativeDetails;

      if (isSpecialNeedsUser(nextEmployeeToPick) && !routeIsCurrentlySpecialNeeds) {
        routeIsCurrentlySpecialNeeds = true;
        currentRouteMaxAllowedOccupancy = 2;
        currentRoute.isSpecialNeedsRoute = true;
      }

      const actualPickedIndexInTemp = tempRemainingEmployeesForThisAttempt.findIndex(
        (e) => e.empCode === nextEmployeeToPick.empCode
      );
      if (actualPickedIndexInTemp > -1) {
        tempRemainingEmployeesForThisAttempt.splice(actualPickedIndexInTemp, 1);
      }
    }

    let routeNeedsGuard = false;
    let finalRouteSpecialNeedsStatusAfterGuard = routeIsCurrentlySpecialNeeds;

    if (guard && currentRoute.employees.length > 0) {
      const critIdx = isDropoff ? currentRoute.employees.length - 1 : 0;
      if (
        currentRoute.employees[critIdx].gender === "F" &&
        !currentRoute.employees.some((e) => e.gender === "M")
      ) {
        routeNeedsGuard = true;
        let capacityTargetForGuardTrim = finalRouteSpecialNeedsStatusAfterGuard
          ? 2
          : Math.max(1, originalPhysicalCapacity - 1);

        while (currentRoute.employees.length > capacityTargetForGuardTrim) {
          const removed = currentRoute.employees.pop();
          if (removed) {
            tempRemainingEmployeesForThisAttempt.unshift(removed);
            if (
              isSpecialNeedsUser(removed) &&
              !currentRoute.employees.some(isSpecialNeedsUser)
            ) {
              finalRouteSpecialNeedsStatusAfterGuard = false;
              capacityTargetForGuardTrim = routeNeedsGuard
                ? Math.max(1, originalPhysicalCapacity - 1)
                : originalPhysicalCapacity;
            }
          } else {
            break;
          }
        }

        // Recalculate route details after guard removal
        if (currentRoute.employees.length > 0) {
          const newCoords = currentRoute.employees.map(e => [e.location.lat, e.location.lng]);
          const newAllCoords = isDropoff 
            ? [facilityCoordinates, ...newCoords]
            : [...newCoords, facilityCoordinates];
          
          const recalcDetails = await calculateRouteDetails(
            newAllCoords,
            currentRoute.employees,
            pickupTimePerEmployee,
            tripType
          );

          if (!recalcDetails.error) {
            currentRoute.routeDetails = recalcDetails;
          }
        }
      }
    }

    if (currentRoute.employees.length > 0) {
      currentRoute.guardNeeded = routeNeedsGuard;
      currentRoute.isSpecialNeedsRoute = finalRouteSpecialNeedsStatusAfterGuard;
      currentRoute.vehicleCapacity = finalRouteSpecialNeedsStatusAfterGuard
        ? 2
        : routeNeedsGuard
        ? Math.max(1, originalPhysicalCapacity - 1)
        : originalPhysicalCapacity;

      routes.push(currentRoute);
    }

    globalRemainingEmployees = tempRemainingEmployeesForThisAttempt;
  }

  // Add deferred employees back to global pool
  if (deferredEmployees.length > 0) {
    globalRemainingEmployees.push(...deferredEmployees);
  }

  return { routes };
}

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
    throw error;
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
      return { routes: [], droppedEmployees: zoneEmployees };
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
      numVehiclesForSolver = numCustomers > 0 ? numCustomers : 1;
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
        facilityData.profile?.allowDroppingVisitsForProblematicZones || true,
      drop_visit_penalty: facilityData.profile?.dropPenalty || 36000,
      facility_coords: [facilityLocation.lat, facilityLocation.lng],
      trip_type: tripType.toUpperCase(),
      direction_penalty_weight:
        facilityData.profile?.directionPenaltyWeight || 2.0,
    };

    const pythonExecutable = "python";
    const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py");

    if (!fs.existsSync(scriptPath)) {
      console.error(
        `[OR-TOOLS SOLVER] Python solver script not found at: ${scriptPath}`
      );
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
        resolve({ routes: orRoutes, droppedEmployees });
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
    return { routes: [], droppedEmployees: [...zoneEmployees] };
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
      pickupTimePerEmployee = 180,
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

    const ensureSpecialFlags = (emp) => ({
      ...emp,
      isMedical: emp.isMedical || false,
      isPWD: emp.isPWD || false,
    });

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
      employeesByZone = assignEmployeesToZones(
        employees.map(ensureSpecialFlags),
        zones
      );
      if (Object.keys(employeesByZone).length === 0 && employees.length > 0) {
        if (!employeesByZone["DEFAULT_ZONE"])
          employeesByZone["DEFAULT_ZONE"] = [];
        employees.forEach((emp) => {
          if (
            !Object.values(employeesByZone)
              .flat()
              .find((e) => e.empCode === emp.empCode)
          ) {
            employeesByZone["DEFAULT_ZONE"].push({
              ...ensureSpecialFlags(emp),
              zone: "DEFAULT_ZONE",
              location: { lat: emp.geoY, lng: emp.geoX },
            });
          }
        });
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
    const allInitiallyFormedRoutes = [];
    let unroutedByOrTools = [];
    let unroutableDueToDeviation = [];

    const isDropoff = tripType.toLowerCase() === "dropoff";
    const facilityCoordinates = [facility.geoY, facility.geoX];

    const processZoneOrGroup = async (
      empsInScope,
      zoneIdentifier,
      effectiveMaxCapacity
    ) => {
      if (empsInScope.length === 0) return;
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
        route.zone = zoneIdentifier;
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

    const allOptimizedOrToolsRoutes = [];
    for (const initialRoute of allInitiallyFormedRoutes) {
      if (!initialRoute.employees || initialRoute.employees.length === 0)
        continue;
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
            true
          );
        if (droppedEmployees && droppedEmployees.length > 0)
          unroutedByOrTools.push(...droppedEmployees);
        if (orToolsSolvedRouteList && orToolsSolvedRouteList.length > 0) {
          allOptimizedOrToolsRoutes.push({
            ...initialRoute,
            employees: orToolsSolvedRouteList[0].employees,
          });
        } else {
          allOptimizedOrToolsRoutes.push(initialRoute);
        }
      } catch (error) {
        console.error(
          `  [OR-Tools Stage] Error optimizing route for zone ${initialRoute.zone}: ${error.message}. Using original order.`
        );
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
          continue;
        }

        const routeCoordinates = route.employees.map((emp) => [
          emp.location.lat,
          emp.location.lng,
        ]);
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
          unroutedByOrTools.push(...route.employees);
          continue;
        }
        updateRouteWithDetails(route, currentRouteDetails);

        let routeActuallyNeedsExternalGuard = false;
        let performReOptimization = false;
        const nightShiftActive = isNightShiftForGuard(
          shiftTime,
          tripType,
          profile
        );

        if (guard && route.employees.length > 0 && nightShiftActive) {
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
            route.guardNeeded = false;
            updateRouteWithDetails(route, guardSwapResult.routeDetails);
            currentRouteDetails = guardSwapResult.routeDetails;
            performReOptimization = true;
          } else if (guardSwapResult.guardNeeded) {
            routeActuallyNeedsExternalGuard = true;
            route.guardNeeded = true;
          } else {
            const critIdx = isDropoff ? route.employees.length - 1 : 0;
            if (
              route.employees.length > 0 &&
              route.employees[critIdx].gender === "F" &&
              !route.employees.some((e) => e.gender === "M")
            ) {
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
          const capacityForReOpt = route.isSpecialNeedsRoute
            ? 2
            : route.vehicleCapacity;
          const reOptResult = await reOptimizeSwappedRouteWithORTools(
            { ...route, vehicleCapacity: capacityForReOpt },
            facility,
            pickupTimePerEmployee
          );
          if (reOptResult.reOptimized && reOptResult.employees.length > 0) {
            route.employees = reOptResult.employees;
            const reOptRouteCoordinates = route.employees.map((emp) => [
              emp.location.lat,
              emp.location.lng,
            ]);
            const reOptAllCoordinates = isDropoff
              ? [facilityCoordinates, ...reOptRouteCoordinates]
              : [...reOptRouteCoordinates, facilityCoordinates];
            currentRouteDetails = await calculateRouteDetails(
              reOptAllCoordinates,
              route.employees,
              pickupTimePerEmployee,
              tripType
            );
            if (currentRouteDetails.error) {
              assignErrorState(
                route,
                `OSRM /trip failed after re-optimization: ${currentRouteDetails.error}`
              );
              unroutableDueToDeviation.push(...route.employees);
              continue;
            } else {
              updateRouteWithDetails(route, currentRouteDetails);
            }
          }
        }

        let tempRemovedForGuardThisRouteIteration = [];
        if (routeActuallyNeedsExternalGuard) {
          let passengerCapacity;
          const capacityBasisForGuardLogic = route.vehicleCapacity;
          if (route.isSpecialNeedsRoute) {
            passengerCapacity = 1;
          } else {
            passengerCapacity = Math.max(1, capacityBasisForGuardLogic - 1);
          }

          if (route.employees.length > passengerCapacity) {
            const numToRemove = route.employees.length - passengerCapacity;
            for (let i = 0; i < numToRemove; i++) {
              if (route.employees.length === 0) break;
              const removedEmp = isDropoff
                ? route.employees.shift()
                : route.employees.pop();
              if (removedEmp) {
                tempRemovedForGuardThisRouteIteration.push(removedEmp);
                if (
                  isSpecialNeedsUser(removedEmp) &&
                  !route.employees.some(isSpecialNeedsUser)
                ) {
                  route.isSpecialNeedsRoute = false;
                }
              }
            }
            if (route.employees.length > 0) {
              const newCoords = route.employees.map((e) => [
                e.location.lat,
                e.location.lng,
              ]);
              const newAllCoordsForGuardTrim = isDropoff
                ? [facilityCoordinates, ...newCoords]
                : [...newCoords, facilityCoordinates];
              const recalcDetailsAfterGuardTrim = await calculateRouteDetails(
                newAllCoordsForGuardTrim,
                route.employees,
                pickupTimePerEmployee,
                tripType
              );
              if (!recalcDetailsAfterGuardTrim.error) {
                updateRouteWithDetails(route, recalcDetailsAfterGuardTrim);
                currentRouteDetails = recalcDetailsAfterGuardTrim;
              } else {
                assignErrorState(
                  route,
                  `OSRM failed after guard removal: ${recalcDetailsAfterGuardTrim.error}`
                );
                unroutableDueToDeviation.push(
                  ...route.employees,
                  ...tempRemovedForGuardThisRouteIteration
                );
                continue;
              }
            } else {
              assignErrorState(route, "No employees after guard removal");
              removedForGuardByZone[route.zone] = (
                removedForGuardByZone[route.zone] || []
              ).concat(tempRemovedForGuardThisRouteIteration);
              continue;
            }
          }
          route.vehicleCapacity = passengerCapacity;
        }
        if (tempRemovedForGuardThisRouteIteration.length > 0) {
          removedForGuardByZone[route.zone] = (
            removedForGuardByZone[route.zone] || []
          ).concat(tempRemovedForGuardThisRouteIteration);
        }

        if (route.employees.length === 0) {
          if (!route.error)
            assignErrorState(
              route,
              "Route became empty after guard/other processing"
            );
          continue;
        }
        if (route.error) {
          unroutableDueToDeviation.push(...route.employees);
          continue;
        }

        const deviationOkay = checkRouteDeviation(route, facility, profile);
        if (!deviationOkay) {
          console.warn(
            `Route ${
              route.routeNumber
            } (Emps: ${route.employees
              .map((e) => e.empCode)
              .join(",")}) FAILED acceptable route deviation. Adding employees to unroutable list.`
          );
          unroutableDueToDeviation.push(...route.employees);
          continue;
        }

        calculatePickupTimes(
          route,
          shiftTime,
          pickupTimePerEmployee,
          reportingTime
        );
        if (
          profileMaxDuration &&
          route.routeDetails &&
          route.routeDetails.totalDuration > profileMaxDuration // Check totalDuration
        ) {
          route.durationExceeded = true;
          // console.warn(`Route ${route.routeNumber} exceeds max duration. Adding to unroutable.`);
          // unroutableDueToDeviation.push(...route.employees);
          // continue;
        }
        route.swapped = routeModifiedByGuardSwap;
        finalProcessedRoutes.push(route);
      } catch (error) {
        console.error(
          `Critical error in post-processing loop for route ${
            route?.routeNumber || "UNKNOWN"
          }:`,
          error
        );
        if (route && route.employees && route.employees.length > 0) {
          unroutableDueToDeviation.push(...route.employees);
        }
      }
    }

    routeData.routeData = [...finalProcessedRoutes];

    let collectedUnroutedForReinsertion = [
      ...unroutedByOrTools,
      ...unroutableDueToDeviation,
    ];
    for (const empList of Object.values(removedForGuardByZone)) {
      collectedUnroutedForReinsertion.push(...empList);
    }
    const potentiallyUnroutedMap = new Map(
      collectedUnroutedForReinsertion.map((emp) => [emp.empCode, emp])
    );
    const successfullyRoutedEmpCodesInMainPass = new Set();
    finalProcessedRoutes.forEach((route) => {
      if (!route.error && route.employees) {
        route.employees.forEach((emp) =>
          successfullyRoutedEmpCodesInMainPass.add(emp.empCode)
        );
      }
    });
    const finalUnroutedEmployees = Array.from(
      potentiallyUnroutedMap.values()
    ).filter((emp) => !successfullyRoutedEmpCodesInMainPass.has(emp.empCode));

    if (finalUnroutedEmployees.length > 0) {
      console.log(`\n[UNROUTED HANDLING] Processing ${finalUnroutedEmployees.length} unrouted employees...`);
      
      // Enforce max capacity for unrouted grouping
      const maxCapacity = getZoneCapacity("GLOBAL", profile);
      const groupedUnrouted = [];
      const remainingUnrouted = [...finalUnroutedEmployees];
      
      while (remainingUnrouted.length > 0) {
        const currentGroup = [];
        const firstEmp = remainingUnrouted.shift();
        currentGroup.push(firstEmp);
        
        for (let i = remainingUnrouted.length - 1; i >= 0; i--) {
          if (currentGroup.length >= maxCapacity) break;
          const candidateEmp = remainingUnrouted[i];
          const tentativeRoute = {
            employees: [...currentGroup, candidateEmp],
            vehicleCapacity: maxCapacity,
            guardNeeded: false,
            isSpecialNeedsRoute: currentGroup.some(isSpecialNeedsUser) || isSpecialNeedsUser(candidateEmp),
            tripType: tripType
          };
          const routeCoords = tentativeRoute.employees.map(emp => [emp.location.lat, emp.location.lng]);
          const allCoords = isDropoff
            ? [facilityCoordinates, ...routeCoords]
            : [...routeCoords, facilityCoordinates];
          const routeDetails = await calculateRouteDetails(
            allCoords,
            tentativeRoute.employees,
            pickupTimePerEmployee,
            tripType
          );
          if (!routeDetails.error) {
            updateRouteWithDetails(tentativeRoute, routeDetails);
            const deviationOkay = checkRouteDeviation(tentativeRoute, facility, profile);
            if (deviationOkay) {
              if (!profileMaxDuration || routeDetails.totalDuration <= profileMaxDuration) {
                currentGroup.push(candidateEmp);
                remainingUnrouted.splice(i, 1);
              }
            }
          }
        }
        if (currentGroup.length > 0) {
          groupedUnrouted.push(currentGroup);
        }
      }
      // Process grouped unrouted employees
      for (const group of groupedUnrouted) {
        totalRouteCount++;
        let groupRoute = {
          routeNumber: totalRouteCount,
          employees: group,
          zone: group[0].zone || "UNROUTED_GROUP",
          vehicleCapacity: maxCapacity,
          guardNeeded: false,
          isSpecialNeedsRoute: group.some(isSpecialNeedsUser),
          uniqueKey: `${group[0].zone}_${totalRouteCount}_${uuidv4()}`,
          tripType: tripType
        };
        // OR-Tools optimization for the group
        try {
          const { routes: orToolsRoutes } = await solveZoneWithORTools(
            group,
            facility,
            maxCapacity,
            profileMaxDuration,
            pickupTimePerEmployee,
            tripType,
            groupRoute.zone,
            true
          );
          if (orToolsRoutes && orToolsRoutes.length > 0) {
            groupRoute.employees = orToolsRoutes[0].employees;
          }
        } catch (e) {
          console.warn(`OR-Tools failed for unrouted group: ${e.message}`);
        }
        // Validate group route with OSRM
        const routeCoords = groupRoute.employees.map(emp => [emp.location.lat, emp.location.lng]);
        const allCoords = isDropoff
          ? [facilityCoordinates, ...routeCoords]
          : [...routeCoords, facilityCoordinates];
        const routeDetails = await calculateRouteDetails(
          allCoords,
          groupRoute.employees,
          pickupTimePerEmployee,
          tripType
        );
        if (routeDetails.error) {
          console.warn(`Group route failed OSRM validation: ${routeDetails.error}`);
          assignErrorState(groupRoute, `OSRM validation failed: ${routeDetails.error}`);
          routeData.routeData.push(groupRoute);
          continue;
        }
        updateRouteWithDetails(groupRoute, routeDetails);
        // Check deviation for group route
        const deviationOkay = checkRouteDeviation(groupRoute, facility, profile);
        if (!deviationOkay) {
          console.warn(`Group route failed deviation check`);
          assignErrorState(groupRoute, "Exceeded acceptable route deviation");
          routeData.routeData.push(groupRoute);
          continue;
        }
        // Guard logic and swapping for unrouted group
        let routeActuallyNeedsExternalGuard = false;
        let performReOptimization = false;
        const nightShiftActive = isNightShiftForGuard(
          shiftTime,
          tripType,
          profile
        );
        if (guard && groupRoute.employees.length > 0 && nightShiftActive) {
          const guardSwapResult = await handleGuardRequirements(
            groupRoute,
            isDropoff,
            facility,
            pickupTimePerEmployee
          );
          if (
            guardSwapResult.swapped &&
            guardSwapResult.routeDetails &&
            !guardSwapResult.routeDetails.error
          ) {
            groupRoute.guardNeeded = false;
            updateRouteWithDetails(groupRoute, guardSwapResult.routeDetails);
            performReOptimization = true;
          } else if (guardSwapResult.guardNeeded) {
            routeActuallyNeedsExternalGuard = true;
            groupRoute.guardNeeded = true;
          } else {
            const critIdx = isDropoff ? groupRoute.employees.length - 1 : 0;
            if (
              groupRoute.employees.length > 0 &&
              groupRoute.employees[critIdx].gender === "F" &&
              !groupRoute.employees.some((e) => e.gender === "M")
            ) {
              routeActuallyNeedsExternalGuard = true;
              groupRoute.guardNeeded = true;
            } else {
              groupRoute.guardNeeded = false;
            }
          }
        } else if (guard && groupRoute.employees.length > 0 && !nightShiftActive) {
          groupRoute.guardNeeded = false;
        }
        if (performReOptimization) {
          const capacityForReOpt = groupRoute.isSpecialNeedsRoute
            ? 2
            : groupRoute.vehicleCapacity;
          const reOptResult = await reOptimizeSwappedRouteWithORTools(
            { ...groupRoute, vehicleCapacity: capacityForReOpt },
            facility,
            pickupTimePerEmployee
          );
          if (reOptResult.reOptimized && reOptResult.employees.length > 0) {
            groupRoute.employees = reOptResult.employees;
            const reOptRouteCoordinates = groupRoute.employees.map((emp) => [
              emp.location.lat,
              emp.location.lng,
            ]);
            const reOptAllCoordinates = isDropoff
              ? [facilityCoordinates, ...reOptRouteCoordinates]
              : [...reOptRouteCoordinates, facilityCoordinates];
            const currentRouteDetails = await calculateRouteDetails(
              reOptAllCoordinates,
              groupRoute.employees,
              pickupTimePerEmployee,
              tripType
            );
            if (!currentRouteDetails.error) {
              updateRouteWithDetails(groupRoute, currentRouteDetails);
            }
          }
        }
        // Calculate pickup times
        calculatePickupTimes(
          groupRoute,
          shiftTime,
          pickupTimePerEmployee,
          reportingTime
        );
        // Check duration
        if (profileMaxDuration && groupRoute.routeDetails.totalDuration > profileMaxDuration) {
          groupRoute.durationExceeded = true;
        }
        routeData.routeData.push(groupRoute);
      }
      // Create singleton routes for any remaining unrouted employees
      for (const unroutedEmp of remainingUnrouted) {
        totalRouteCount++;
        let singletonRoute = {
          routeNumber: totalRouteCount,
          employees: [unroutedEmp],
          zone: unroutedEmp.zone || "UNROUTED_SINGLETON",
          vehicleCapacity: 1,
          guardNeeded: false,
          isSpecialNeedsRoute: isSpecialNeedsUser(unroutedEmp),
          uniqueKey: `${unroutedEmp.zone}_${totalRouteCount}_${uuidv4()}`,
          tripType: tripType
        };
        // OR-Tools optimization for singleton
        try {
          const { routes: orToolsRoutes } = await solveZoneWithORTools(
            [unroutedEmp],
            facility,
            1,
            profileMaxDuration,
            pickupTimePerEmployee,
            tripType,
            singletonRoute.zone,
            true
          );
          if (orToolsRoutes && orToolsRoutes.length > 0) {
            singletonRoute.employees = orToolsRoutes[0].employees;
          }
        } catch (e) {
          console.warn(`OR-Tools failed for singleton unrouted: ${e.message}`);
        }
        // Validate singleton route with OSRM
        const routeCoords = singletonRoute.employees.map(emp => [emp.location.lat, emp.location.lng]);
        const allCoords = isDropoff
          ? [facilityCoordinates, ...routeCoords]
          : [...routeCoords, facilityCoordinates];
        const routeDetails = await calculateRouteDetails(
          allCoords,
          singletonRoute.employees,
          pickupTimePerEmployee,
          tripType
        );
        if (routeDetails.error) {
          console.warn(`Singleton route for ${unroutedEmp.empCode} failed OSRM validation: ${routeDetails.error}`);
          assignErrorState(singletonRoute, `OSRM validation failed: ${routeDetails.error}`);
          routeData.routeData.push(singletonRoute);
          continue;
        }
        updateRouteWithDetails(singletonRoute, routeDetails);
        // Check deviation for singleton route
        const deviationOkay = checkRouteDeviation(singletonRoute, facility, profile);
        if (!deviationOkay) {
          console.warn(`Singleton route for ${unroutedEmp.empCode} failed deviation check`);
          assignErrorState(singletonRoute, "Exceeded acceptable route deviation");
          routeData.routeData.push(singletonRoute);
          continue;
        }
        // Guard logic and swapping for singleton
        let routeActuallyNeedsExternalGuard = false;
        let performReOptimization = false;
        const nightShiftActive = isNightShiftForGuard(
          shiftTime,
          tripType,
          profile
        );
        if (guard && singletonRoute.employees.length > 0 && nightShiftActive) {
          const guardSwapResult = await handleGuardRequirements(
            singletonRoute,
            isDropoff,
            facility,
            pickupTimePerEmployee
          );
          if (
            guardSwapResult.swapped &&
            guardSwapResult.routeDetails &&
            !guardSwapResult.routeDetails.error
          ) {
            singletonRoute.guardNeeded = false;
            updateRouteWithDetails(singletonRoute, guardSwapResult.routeDetails);
            performReOptimization = true;
          } else if (guardSwapResult.guardNeeded) {
            routeActuallyNeedsExternalGuard = true;
            singletonRoute.guardNeeded = true;
          } else {
            const critIdx = isDropoff ? singletonRoute.employees.length - 1 : 0;
            if (
              singletonRoute.employees.length > 0 &&
              singletonRoute.employees[critIdx].gender === "F" &&
              !singletonRoute.employees.some((e) => e.gender === "M")
            ) {
              routeActuallyNeedsExternalGuard = true;
              singletonRoute.guardNeeded = true;
            } else {
              singletonRoute.guardNeeded = false;
            }
          }
        } else if (guard && singletonRoute.employees.length > 0 && !nightShiftActive) {
          singletonRoute.guardNeeded = false;
        }
        if (performReOptimization) {
          const capacityForReOpt = singletonRoute.isSpecialNeedsRoute
            ? 2
            : singletonRoute.vehicleCapacity;
          const reOptResult = await reOptimizeSwappedRouteWithORTools(
            { ...singletonRoute, vehicleCapacity: capacityForReOpt },
            facility,
            pickupTimePerEmployee
          );
          if (reOptResult.reOptimized && reOptResult.employees.length > 0) {
            singletonRoute.employees = reOptResult.employees;
            const reOptRouteCoordinates = singletonRoute.employees.map((emp) => [
              emp.location.lat,
              emp.location.lng,
            ]);
            const reOptAllCoordinates = isDropoff
              ? [facilityCoordinates, ...reOptRouteCoordinates]
              : [...reOptRouteCoordinates, facilityCoordinates];
            const currentRouteDetails = await calculateRouteDetails(
              reOptAllCoordinates,
              singletonRoute.employees,
              pickupTimePerEmployee,
              tripType
            );
            if (!currentRouteDetails.error) {
              updateRouteWithDetails(singletonRoute, currentRouteDetails);
            }
          }
        }
        // Calculate pickup times
        calculatePickupTimes(
          singletonRoute,
          shiftTime,
          pickupTimePerEmployee,
          reportingTime
        );
        // Check duration
        if (profileMaxDuration && singletonRoute.routeDetails.totalDuration > profileMaxDuration) {
          singletonRoute.durationExceeded = true;
        }
        routeData.routeData.push(singletonRoute);
      }
    }

    // After all routing and singleton/group handling
    // Collect all routed employee codes
    const routedEmpCodes = new Set();
    routeData.routeData.forEach(route => {
      if (!route.error && route.employees) {
        route.employees.forEach(emp => routedEmpCodes.add(emp.empCode));
      }
    });
    // Find unrouted employees
    const unroutedEmployees = employees.filter(emp => !routedEmpCodes.has(emp.empCode));

    const stats = calculateRouteStatistics(routeData, employees.length);
    const response = createSimplifiedResponse({
      ...routeData,
      ...stats,
      totalSwappedRoutes: finalTotalSwappedRoutes,
    });
    // Add unrouted employees to the response
    response.unroutedEmployees = unroutedEmployees.map(emp => ({
      empCode: emp.empCode,
      geoX: emp.geoX,
      geoY: emp.geoY,
      gender: emp.gender,
      isMedical: emp.isMedical || false,
      isPWD: emp.isPWD || false
    }));

    if (saveToDatabase) {
      /* console.log("Simulating save to database"); */
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

function formatTime(date) {
  if (!(date instanceof Date) || isNaN(date.getTime())) {
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
  reportingTimeSeconds = 0
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

    const isDropoff = route.tripType?.toLowerCase() === "dropoff";
    let currentTime = new Date(facilityTargetTime);

    if (!isDropoff) {
      let targetFacilityArrivalTime = new Date(facilityTargetTime);
      if (reportingTimeSeconds > 0) {
        targetFacilityArrivalTime.setSeconds(
          targetFacilityArrivalTime.getSeconds() - reportingTimeSeconds
        );
      }
      route.facilityArrivalTime = formatTime(targetFacilityArrivalTime);
      currentTime = new Date(targetFacilityArrivalTime);

      for (let i = route.employees.length - 1; i >= 0; i--) {
        const employee = route.employees[i];
        const legToNextStopOrFacility = route.routeDetails?.legs?.[i];
        const legDuration =
          (legToNextStopOrFacility?.duration || 0) *
          (1 + TRAFFIC_BUFFER_PERCENTAGE);

        currentTime.setSeconds(currentTime.getSeconds() - legDuration);
        currentTime.setSeconds(
          currentTime.getSeconds() - pickupTimePerEmployee
        );
        employee.pickupTime = formatTime(currentTime);
      }
    } else {
      route.facilityDepartureTime = formatTime(currentTime);

      for (let i = 0; i < route.employees.length; i++) {
        const employee = route.employees[i];
        const legToThisEmployee = route.routeDetails?.legs?.[i];
        const legDuration =
          (legToThisEmployee?.duration || 0) * (1 + TRAFFIC_BUFFER_PERCENTAGE);

        currentTime.setSeconds(currentTime.getSeconds() + legDuration);
        currentTime.setSeconds(
          currentTime.getSeconds() + pickupTimePerEmployee
        );
        employee.dropoffTime = formatTime(currentTime);
        employee.pickupTime = employee.dropoffTime;
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
    if (!route?.employees?.length || route.employees.length < 2) {
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
      return { guardNeeded: true, swapped: false };
    }

    const osrmCoordinates = [
      `${criticalEmployee.location.lng},${criticalEmployee.location.lat}`,
      ...potentialMaleCandidates.map(
        (emp) => `${emp.location.lng},${emp.location.lat}`
      ),
    ];

    const sources = "0";
    const destinations = potentialMaleCandidates
      .map((_, i) => i + 1)
      .join(";");

    const osrmTableUrl = `http://localhost:5000/table/v1/driving/${osrmCoordinates.join(
      ";"
    )}?sources=${sources}&destinations=${destinations}&annotations=distance`;

    let osrmDistances = [];
    try {
      const response = await fetchApi(osrmTableUrl, {
        timeout: OSRM_PROBE_TIMEOUT_HEURISTIC,
      });
      if (response.ok) {
        const data = await response.json();
        if (data.code === "Ok" && data.distances && data.distances.length > 0) {
          osrmDistances = data.distances[0];
        } else {
          console.warn(
            `[handleGuardRequirements] OSRM /table error or no distances for route ${
              route.routeNumber
            }: ${data.code} - ${data.message || ""}`
          );
          return { guardNeeded: true, swapped: false };
        }
      } else {
        console.warn(
          `[handleGuardRequirements] OSRM /table HTTP error ${response.status} for route ${route.routeNumber}`
        );
        return { guardNeeded: true, swapped: false };
      }
    } catch (error) {
      console.error(
        `[handleGuardRequirements] OSRM /table fetch error for route ${route.routeNumber}:`,
        error
      );
      return { guardNeeded: true, swapped: false };
    }

    const validCandidates = [];
    potentialMaleCandidates.forEach((maleEmp, idx) => {
      const roadDistanceMeters = osrmDistances[idx];
      if (roadDistanceMeters != null) {
        const roadDistanceKm = roadDistanceMeters / 1000;
        if (roadDistanceKm <= MAX_SWAP_DISTANCE_KM) {
          validCandidates.push({
            employee: maleEmp,
            index: route.employees.findIndex(
              (e) => e.empCode === maleEmp.empCode
            ),
            distance: roadDistanceKm,
          });
        }
      }
    });

    if (validCandidates.length === 0) {
      return { guardNeeded: true, swapped: false };
    }

    validCandidates.sort((a, b) => a.distance - b.distance);
    const bestCandidate = validCandidates[0];

    const newEmployees = [...route.employees];
    if (bestCandidate.index === -1 || bestCandidate.index === checkIndex) {
      console.error(
        `[handleGuardRequirements] Error finding original index for best candidate or candidate is the critical employee.`
      );
      return { guardNeeded: true, swapped: false };
    }

    [newEmployees[checkIndex], newEmployees[bestCandidate.index]] = [
      newEmployees[bestCandidate.index],
      newEmployees[checkIndex],
    ];

    const newRouteCoordinates = newEmployees.map((emp) => [
      emp.location.lat,
      emp.location.lng,
    ]);
    const facilityCoordsArray = [facility.geoY, facility.geoX];
    const allCoordinatesForTrip = isDropoff
      ? [facilityCoordsArray, ...newRouteCoordinates]
      : [...newRouteCoordinates, facilityCoordsArray];

    const routeDetailsAfterSwap = await calculateRouteDetails(
      allCoordinatesForTrip,
      newEmployees,
      pickupTimePerEmployee,
      route.tripType
    );

    if (routeDetailsAfterSwap.error) {
      console.warn(
        `Swap validation (OSRM /trip) failed for route ${route.routeNumber} after road distance swap: ${routeDetailsAfterSwap.error}`
      );
      return { guardNeeded: true, swapped: false };
    }

    return {
      guardNeeded: false,
      swapped: true,
      routeDetails: routeDetailsAfterSwap,
    };
  } catch (error) {
    console.error(
      `Error in handleGuardRequirements (road distance) for route ${route?.routeNumber}:`,
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
      return { viable: false, routeDetails };
    }

    const originalDuration = route.routeDetails?.totalDuration || Infinity; // Use totalDuration
    const newDuration = routeDetails.totalDuration;
    const durationIncrease =
      newDuration > originalDuration
        ? (newDuration - originalDuration) / originalDuration
        : 0;

    return {
      viable: durationIncrease <= 0.2,
      routeDetails,
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
  route.routeDetails = { totalDistance: 0, totalDuration: 0, legs: [] }; // Use totalDistance/totalDuration
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
    totalDistance: routeDetails.totalDistance,
    totalDuration: routeDetails.totalDuration,
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
    // Access totalDistance and totalDuration from route.routeDetails
    const routeDist = route.routeDetails?.totalDistance;
    const routeDur = route.routeDetails?.totalDuration;

    if (routeDur !== Infinity && routeDist !== Infinity) {
      totalDistanceSum += routeDist || 0;
      totalDurationSum += routeDur || 0;
    }
  });

  return {
    totalEmployees: totalEmployeesInput,
    totalRoutedEmployees,
    totalRoutes: totalValidRoutes,
    averageOccupancy: parseFloat(averageOccupancy.toFixed(2)),
    routeDetails: { // This becomes overallRouteDetails in the response
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
    overallRouteDetails: routeData.routeDetails, // This now comes from calculateRouteStatistics
    totalSwappedRoutes: routeData.totalSwappedRoutes,
    routes: routeData.routeData
      .filter((route) => !route.error && route.employees?.length > 0)
      .map((route) => {
        const guardAssigned = route.guardNeeded || false;
        const occupancy = (route.employees?.length || 0) + (guardAssigned ? 1 : 0);
        const reportedVehicleCapacity = route.vehicleCapacity;

        return {
          routeNumber: route.routeNumber,
          zone: route.zone,
          vehicleCapacity: reportedVehicleCapacity,
          guard: guardAssigned,
          swapped: route.swapped || false,
          durationExceeded: route.durationExceeded || false,
          uniqueKey: route.uniqueKey,
          isSpecialNeedsRoute: route.isSpecialNeedsRoute || false,
          // Access totalDistance and totalDuration for individual routes
          distance: parseFloat(
            ((route.routeDetails?.totalDistance || 0) / 1000).toFixed(2)
          ),
          duration: parseFloat(
            (route.routeDetails?.totalDuration || 0).toFixed(2)
          ),
          occupancy,
          encodedPolyline: route.encodedPolyline || "no_polyline",
          employees: (route.employees || []).map((emp, index) => ({
            empCode: emp.empCode,
            gender: emp.gender,
            isMedical: emp.isMedical || false,
            isPWD: emp.isPWD || false,
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
