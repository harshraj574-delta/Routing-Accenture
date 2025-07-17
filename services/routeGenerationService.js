const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const { spawn } = require("child_process"); // For calling Python

// const TRAFFIC_BUFFER_PERCENTAGE = 0.4; // 40% buffer for traffic
const MAX_SWAP_DISTANCE_KM = 1.5; // or your business threshold

const OSRM_PROBE_TIMEOUT_HEURISTIC = 3000;
const OSRM_PROBE_TIMEOUT = 8000;
const BYPASS_ROUTE_DEVIATION_CHECKS = false; // Set to true to bypass all deviation checks



/**
 * Calculate traffic buffer percentage based on shift time
 * @param {string|number} shiftTime - Time in format "0930" or 930
 * @returns {number} Traffic buffer percentage (0.0 to 1.0)
 */
function getTrafficBufferForShiftTime(shiftTime) {
  if (!shiftTime) {
    return 0.4; // Default 40% buffer
  }

  // Convert to 24-hour format number
  const timeStr = shiftTime.toString().padStart(4, "0");
  const hours = parseInt(timeStr.substring(0, 2), 10);
  const minutes = parseInt(timeStr.substring(2, 4), 10);

  if (isNaN(hours) || isNaN(minutes)) {
    console.warn(
      `[Traffic Buffer] Invalid shift time: ${shiftTime}. Using default buffer.`
    );
    return 0.4;
  }

  // Convert to decimal hours for easier comparison
  const decimalTime = hours + minutes / 60;

  // Define traffic patterns based on time of day
  if (decimalTime >= 7.0 && decimalTime < 10.0) {
    // Morning rush hour: 7:00 AM - 10:00 AM (High traffic)
    return 1.2; // 60% buffer
  } else if (decimalTime >= 10.0 && decimalTime < 16.0) {
    // Afternoon: 10:00 AM - 4:00 PM (Moderate traffic)
    return 1.2; // 30% buffer
  } else if (decimalTime >= 16.0 && decimalTime < 20.0) {
    // Evening rush hour: 4:00 PM - 8:00 PM (High traffic)
    return 1.2; // 60% buffer
  } else {
    // Night time: 8:00 PM - 7:00 AM (Low traffic)
    return 0.9; // 20% buffer
  }
}

const fetchApi = (...args) => {
  return import("node-fetch").then(({ default: fetch }) => fetch(...args));
};


const ZONES_DATA_FILE = path.join(__dirname, "../data/bengaluru_zones.json");

const FASTAPI_GATEWAY_URL = "https://mapapi.etmsonline.in";

function getFastApiCityKey(city) {
  const normalized = city?.toLowerCase();
  if (
    normalized === "ncr" ||
    normalized === "delhi" ||
    normalized === "delhi ncr"
  )
    return "delhi";
  if (normalized === "bengaluru" || normalized === "bangalore")
    return "bangalore";
  if (normalized === "chennai") return "chennai";
  return "delhi"; // default
}

async function isOsrmAvailable(profile) {
  const city = getFastApiCityKey(profile?.name);
  try {
    const response = await fetchApi(`${FASTAPI_GATEWAY_URL}/route`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        city,
        coordinates: [
          [28.7041, 77.1025],
          [28.7042, 77.1026],
        ],
        overview: "false",
        steps: false,
        geometries: "polyline",
      }),
      timeout: 8000,
    });
    if (response.ok) {
      const data = await response.json();
      return data && data.code === "Ok";
    }
    return false;
  } catch (error) {
    console.log("this is it ",error);
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
  if (!point || !polygon || !Array.isArray(polygon) || polygon.length < 3)
    return false;
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

async function loadZonesData() {
  const data = await fs.promises.readFile(ZONES_DATA_FILE, "utf8");
  const zonesData = JSON.parse(data);
  return zonesData.features;
}

function assignEmployeesToZones(employees, zones) {
  const employeesByZone = {};
  const assignedEmployees = new Set();
  if (!zones || zones.length === 0) {
    console.warn(
      "[assignEmployeesToZones] No zones provided. All employees will be in DEFAULT_ZONE."
    );
  } else {
    for (let i = 0; i < zones.length; i++) {
      const zone = zones[i];
      if (!zone?.properties || !zone.geometry?.coordinates?.[0]) {
        console.warn(
          `[assignEmployeesToZones] Invalid zone structure at index ${i}`,
          zone
        );
        continue;
      }
      const zoneName = zone.properties?.Name || `Unknown Zone ${i}`;
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
  }
  const unassignedEmployees = employees.filter(
    (emp) => !assignedEmployees.has(emp.empCode) && emp.geoX && emp.geoY
  );
  if (unassignedEmployees.length > 0) {
    const defaultZoneName = "DEFAULT_ZONE";
    if (!employeesByZone[defaultZoneName])
      employeesByZone[defaultZoneName] = [];
    employeesByZone[defaultZoneName].push(
      ...unassignedEmployees.map((emp) => ({
        ...emp,
        zone: defaultZoneName,
        location: { lat: emp.geoY, lng: emp.geoX },
      }))
    );
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

// isNightShiftForGuard is kept for potential other uses or future logic changes,
// but not primary for guard assignment if global 'guard' flag is the main driver.
function isNightShiftForGuard(shiftTime, tripType, profile) {
  if (!shiftTime || !tripType) return false;
  const st = parseInt(shiftTime.toString().replace(":", ""), 10);
  if (isNaN(st)) return false;
  let nightShiftConfig = profile?.nightShiftGuardTimings;
  if (!nightShiftConfig) {
    nightShiftConfig = {
      PICKUP: { start: 2000, end: 700 },
      DROPOFF: { start: 1900, end: 530 },
    };
  }
  const facilityTypePrefix = (profile?.facilityType || "CDC").toUpperCase();
  const typeConfigKeyWithFacility = `${facilityTypePrefix}_${tripType.toUpperCase()}`;
  let config =
    nightShiftConfig[typeConfigKeyWithFacility] ||
    nightShiftConfig[tripType.toUpperCase()];
  if (!config) return false;
  const { start, end } = config;
  if (start > end) return st >= start || st <= end;
  if (start < end) return st >= start && st <= end;
  return st === start;
}

const isSpecialNeedsUser = (emp) => {
  if (!emp) return false;
  return emp.isMedical || false || emp.isPWD || false;
};

async function checkRouteDeviation(route, facility, profile) {

   if (BYPASS_ROUTE_DEVIATION_CHECKS) {
    console.log(`[RouteDeviation] BYPASSING deviation check for route ${route.uniqueKey || route.routeNumber} (global override).`);
    return true;
  }
  
  const city = profile?.name;
  const fastApiCity = getFastApiCityKey(city);
  // const osrmBaseUrl = getOsrmBaseUrl(city);

  if (!profile?.routeDeviationRules) return true;
  if (!route?.routeDetails || !route?.employees || route.employees.length === 0)
    return true;

  const ruleKeys = Object.keys(profile.routeDeviationRules);
  if (ruleKeys.length === 0) return true;

  let effectiveRuleKey =
    profile.facilityType && profile.routeDeviationRules[profile.facilityType]
      ? profile.facilityType
      : ruleKeys[0];
  let rules = profile.routeDeviationRules[effectiveRuleKey];

  if (!rules || !Array.isArray(rules) || rules.length === 0) {
    console.warn(
      `[checkRouteDeviation] Route ${
        route.uniqueKey || route.routeNumber
      }: No valid deviation rules for key "${effectiveRuleKey}". Returning true (lenient).`
    );
    return true;
  }

  // CORRECTED: Calculate farthest employee distance using same logic as response
  let farthestEmployeeDistanceKm = 0;
  const isDropoffRoute = route.tripType?.toLowerCase() === "dropoff";

  if (route.employees.length > 0) {
    // Get the farthest employee (same logic as response generation)
    const farthestEmployee = isDropoffRoute
      ? route.employees[route.employees.length - 1] // Last dropoff
      : route.employees[0]; // First pickup

    if (
      farthestEmployee?.location?.lng != null &&
      farthestEmployee?.location?.lat != null
    ) {
      try {
        // Prepare coordinates as [lng, lat] pairs for FastAPI
        const coords = isDropoffRoute
          ? [
              [facility.geoX, facility.geoY],
              [farthestEmployee.location.lng, farthestEmployee.location.lat],
            ]
          : [
              [farthestEmployee.location.lng, farthestEmployee.location.lat],
              [facility.geoX, facility.geoY],
            ];
        const response = await fetchApi(`${FASTAPI_GATEWAY_URL}/route`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            city: fastApiCity,
            coordinates: coords,
            overview: "false",
            steps: false,
            geometries: "polyline",
          }),
          timeout: OSRM_PROBE_TIMEOUT,
        });

        if (response.ok) {
          const data = await response.json();
          if (data.code === "Ok" && data.routes?.[0]?.distance != null) {
            farthestEmployeeDistanceKm = data.routes[0].distance / 1000;
          }
        }
      } catch (err) {
        console.warn(
          `[checkRouteDeviation] OSRM error for farthest employee distance: ${err.message}`
        );
      }
    }
  }

  // Find applicable rule based on farthest employee distance
  const EPSILON = 0.001;
  let applicableRule = rules.find(
    (rule) =>
      farthestEmployeeDistanceKm >= rule.minDistKm - EPSILON &&
      farthestEmployeeDistanceKm <= rule.maxDistKm + EPSILON
  );

  if (!applicableRule) {
    const sortedRules = [...rules].sort((a, b) => a.maxDistKm - b.maxDistKm);
    if (
      sortedRules.length > 0 &&
      farthestEmployeeDistanceKm > sortedRules[sortedRules.length - 1].maxDistKm
    ) {
      applicableRule = sortedRules[sortedRules.length - 1];
    } else if (sortedRules.length > 0) {
      let closestRule = sortedRules[0];
      for (const rule of sortedRules) {
        if (farthestEmployeeDistanceKm <= rule.maxDistKm + EPSILON) {
          closestRule = rule;
          break;
        }
      }
      applicableRule = closestRule;
    }
  }

  if (!applicableRule?.maxTotalOneWayKm) {
    console.warn(
      `[checkRouteDeviation] Could not determine applicable rule for Route ${
        route.uniqueKey || route.routeNumber
      }. Farthest emp dist: ${farthestEmployeeDistanceKm.toFixed(
        3
      )}km. Rules: ${JSON.stringify(rules)}. Returning FALSE.`
    );
    return false;
  }

  const relevantRouteDistanceKm =
    (route.routeDetails.totalDistance || 0) / 1000;

  if (relevantRouteDistanceKm > applicableRule.maxTotalOneWayKm) {
    console.warn(
      `[checkRouteDeviation] Route ${
        route.uniqueKey || route.routeNumber
      }: DEVIATION EXCEEDED. Rule: ${applicableRule.minDistKm}-${
        applicableRule.maxDistKm
      }km (maxTotal: ${
        applicableRule.maxTotalOneWayKm
      }km). FarthestEmpDist: ${farthestEmployeeDistanceKm.toFixed(
        3
      )}km. ActualRouteDist: ${relevantRouteDistanceKm.toFixed(
        3
      )}km. Returning false.`
    );
    return false;
  }

  console.log(
    `[checkRouteDeviation] Route ${
      route.uniqueKey || route.routeNumber
    }: PASSED. Rule: ${applicableRule.minDistKm}-${
      applicableRule.maxDistKm
    }km (maxTotal: ${
      applicableRule.maxTotalOneWayKm
    }km). FarthestEmpDist: ${farthestEmployeeDistanceKm.toFixed(
      3
    )}km. ActualRouteDist: ${relevantRouteDistanceKm.toFixed(3)}km.`
  );
  return true;
}

async function calculateRouteDetails(
  routeCoordinates, // Expects [[lat, lng], ...] from your JS code
  employees, // Already in the desired sequence (from heuristic or OR-Tools)
  pickupTimePerEmployee, // This parameter is not used in the provided snippet, but kept for signature consistency
  tripType = "pickup",
  city,
  shiftTime = null
) {
  const fastApiCity = getFastApiCityKey(city);
  const TRAFFIC_BUFFER_PERCENTAGE = getTrafficBufferForShiftTime(shiftTime);
  try {
    if (!routeCoordinates || routeCoordinates.length === 0) {
      // ... (your existing validation for empty routeCoordinates)
      // No changes needed here
      if (
        (!employees || employees.length === 0) &&
        routeCoordinates &&
        routeCoordinates.length > 1
      ) {
        throw new Error(
          "Invalid input: routeCoordinates has multiple points but employees array is empty/null for /route call."
        );
      } else if (
        !employees &&
        (!routeCoordinates || routeCoordinates.length === 0)
      ) {
        throw new Error(
          "Invalid input: Both routeCoordinates and employees are empty/null for /route call."
        );
      }
      if (
        (!routeCoordinates || routeCoordinates.length === 0) &&
        employees &&
        employees.length > 0
      ) {
        throw new Error(
          "Invalid input: routeCoordinates is empty but employees array is not."
        );
      }
      if (routeCoordinates && routeCoordinates.length < 2) {
        throw new Error(
          "Invalid input: routeCoordinates must contain at least two points for an OSRM /route call."
        );
      }
    }

    // Prepare coordinates as [lng, lat] pairs for FastAPI
    const coords = routeCoordinates.map((c) => [c[1], c[0]]); // [lng, lat]
    const url = `${FASTAPI_GATEWAY_URL}/route`;
    const response = await fetchApi(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        city: fastApiCity,
        coordinates: coords,
        overview: "full",
        steps: true,
        geometries: "polyline",
      }),
    });
    if (!response.ok) {
      const errorText = await response.text();
      console.error(
        `[OSRM /route] HTTP error: ${response.status} for URL: ${url}. Body: ${errorText}`
      );
      throw new Error(`OSRM /route error: ${response.status}`);
    }
    const data = await response.json();
    // Debug log for OSRM response
    // console.log(
    //   "[DEBUG calculateRouteDetails] OSRM response:",
    //   JSON.stringify(data)
    // );
    if (data.routes && data.routes[0]) {
      console.log(
        "[DEBUG calculateRouteDetails] routeObject.distance:",
        data.routes[0].distance
      );
    }
    if (data.code !== "Ok" || !data.routes || data.routes.length === 0) {
      console.error(
        "[OSRM /route] API returned non-Ok code or no routes:",
        data,
        ` for URL: ${url}`
      );
      throw new Error(
        `Invalid OSRM /route response: ${
          data.code || "Unknown code"
        }, Message: ${data.message || "No routes found"}`
      );
    }

    const routeObject = data.routes[0];
    const waypointsFromApi = data.waypoints;
    let fullRoadPolyline = routeObject.geometry || "";

    if (!fullRoadPolyline && routeObject.legs) {
      let fullCoords = [];
      for (const leg of routeObject.legs) {
        if (leg.steps) {
          for (const step of leg.steps) {
            const coords = decodePolyline(step.geometry);
            if (fullCoords.length > 0 && coords.length > 0) {
              const lastPt = fullCoords[fullCoords.length - 1];
              if (
                lastPt &&
                coords[0] &&
                lastPt[0] === coords[0][0] &&
                lastPt[1] === coords[0][1]
              )
                fullCoords.pop();
            }
            fullCoords = fullCoords.concat(coords);
          }
        }
      }
      fullRoadPolyline = encodePolyline(fullCoords);
    }

    let orderedEmployees = [];
    const inputEmployeesOriginal = employees ? [...employees] : [];

    if (waypointsFromApi && inputEmployeesOriginal.length > 0) {
      const inputCoordIndexToEmployeeMap = new Map();
      let employeeIndexTracker = 0;

      for (let i = 0; i < routeCoordinates.length; i++) {
        let isFacilityInRouteCoords = false;
        if (tripType.toLowerCase() === "dropoff" && i === 0)
          isFacilityInRouteCoords = true;
        if (
          tripType.toLowerCase() === "pickup" &&
          i === routeCoordinates.length - 1
        )
          isFacilityInRouteCoords = true;

        if (
          !isFacilityInRouteCoords &&
          employeeIndexTracker < inputEmployeesOriginal.length
        ) {
          inputCoordIndexToEmployeeMap.set(
            i,
            inputEmployeesOriginal[employeeIndexTracker]
          );
          employeeIndexTracker++;
        }
      }

      // console.log(`[DEBUG calculateRouteDetails] TripType: ${tripType}`);
      // console.log(`[DEBUG calculateRouteDetails] inputEmployeesOriginal.length: ${inputEmployeesOriginal.length}`);
      // console.log(`[DEBUG calculateRouteDetails] routeCoordinates:`, JSON.stringify(routeCoordinates));
      // console.log(`[DEBUG calculateRouteDetails] inputCoordIndexToEmployeeMap:`, JSON.stringify(Array.from(inputCoordIndexToEmployeeMap.entries())));
      // console.log(`[DEBUG calculateRouteDetails] waypointsFromApi:`, JSON.stringify(waypointsFromApi));

      // --- MODIFIED WAYPOINT MAPPING ---
      for (let i = 0; i < waypointsFromApi.length; i++) {
        // const wp = waypointsFromApi[i]; // Not needed if not accessing other wp properties
        // Use the current loop index 'i' as the originalInputCoordIndex
        // This assumes OSRM returns waypoints in the same order as input coordinates
        const originalInputCoordIndex = i;

        const employeeForThisWaypoint = inputCoordIndexToEmployeeMap.get(
          originalInputCoordIndex
        );

        // console.log(`[DEBUG calculateRouteDetails] Mapping OSRM waypoint at API index ${i} (used as originalInputCoordIndex), Found employee: ${!!employeeForThisWaypoint}`);

        if (employeeForThisWaypoint) {
          orderedEmployees.push({
            ...employeeForThisWaypoint,
            order: orderedEmployees.length + 1,
          });
        }
      }
      // --- END OF MODIFIED WAYPOINT MAPPING ---

      if (orderedEmployees.length !== inputEmployeesOriginal.length) {
        console.warn(
          `[calculateRouteDetails OSRM /route] Mismatch after mapping waypoints. Expected ${inputEmployeesOriginal.length}, got ${orderedEmployees.length}. Using original employee order.`
        );
        orderedEmployees = inputEmployeesOriginal.map((emp, idx) => ({
          ...emp,
          order: idx + 1,
        }));
      }
    } else if (inputEmployeesOriginal.length > 0) {
      console.warn(
        "[calculateRouteDetails OSRM /route] Waypoints array missing or no employees to map. Using input employee order."
      );
      orderedEmployees = inputEmployeesOriginal.map((emp, idx) => ({
        ...emp,
        order: idx + 1,
      }));
    }

    return {
      employees: orderedEmployees,
      totalDistance: routeObject.distance,
      // --- USE THE PASSED trafficBuffer PARAMETER ---
      totalDuration: routeObject.duration * (1 + TRAFFIC_BUFFER_PERCENTAGE),
      encodedPolyline: fullRoadPolyline,
      legs: routeObject.legs || [],
      geometry: {
        type: "LineString",
        coordinates: decodePolyline(fullRoadPolyline).map((c) => [c[1], c[0]]),
      },
    };
  } catch (error) {
    console.error(
      "calculateRouteDetails (OSRM GET /route) error:",
      error.message,
      error.stack
    );
    const errorEmployees = employees
      ? employees.map((e, i) => ({ ...e, order: i + 1 }))
      : [];
    return {
      employees: errorEmployees,
      totalDistance: 0,
      totalDuration: 0,
      encodedPolyline: "",
      legs: [],
      geometry: null,
      error: error.message,
    };
  }
}

async function generateDistanceDurationMatrix(
  locationsForMatrix,
  facilityLocation,
  city
) {
  const fastApiCity = getFastApiCityKey(city);
  const allPointsCoords = [
    facilityLocation,
    ...locationsForMatrix.map((emp) => emp.location),
  ];
  if (allPointsCoords.length <= 1) {
    return { distanceMatrix: [[]], durationMatrix: [[]], pointMap: [] };
  }
  // Prepare coordinates as [lat, lng] pairs for FastAPI
  const coords = allPointsCoords.map((p) => [p.lat, p.lng]);
  const matrixTimeout = OSRM_PROBE_TIMEOUT + allPointsCoords.length * 200;
  const response = await fetchApi(`${FASTAPI_GATEWAY_URL}/table`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      city: fastApiCity,
      coordinates: coords,
      annotations: "duration,distance",
    }),
    timeout: matrixTimeout,
  });
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `OSRM table service error for matrix: ${
        response.status
      }. Body: ${errorText.substring(0, 200)}`
    );
  }
  const data = await response.json();
  if (data.code !== "Ok" || !data.durations || !data.distances) {
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
}

async function assignVehicleAndFinalizeGroup(
  routeShell,
  preliminaryEmployeesInGroup,
  profile,
  availableFleetCounts,
  shiftTime, // Kept for consistency, not used for guard decision here
  tripType,
  activateGuardSystem // This is the 'guard: true/false' from input JSON
) {
  let currentEmployeesForRoute = [...preliminaryEmployeesInGroup];
  let employeesTrimmedOff = [];

  routeShell.employees = [];
  routeShell.error = false;
  routeShell.errorMessage = "";
  routeShell.afterFleetExhaustion = false;
  routeShell.assignedVehicleType = "NONE";
  routeShell.vehicleCapacity = 0;
  let preliminaryGuardNeeded = false; // Default to no guard

  if (!currentEmployeesForRoute || currentEmployeesForRoute.length === 0) {
    routeShell.guardNeeded = false;
    return { employeesTrimmedOff };
  }

  const isDropoff = tripType.toLowerCase() === "dropoff";

  // CORRECTED: Guard needed if critical position is Female (regardless of other males)
  if (activateGuardSystem && currentEmployeesForRoute.length > 0) {
    const critIdx = isDropoff ? currentEmployeesForRoute.length - 1 : 0;
    if (currentEmployeesForRoute[critIdx]?.gender === "F") {
      preliminaryGuardNeeded = true;
    }
  }
  routeShell.guardNeeded = preliminaryGuardNeeded;

  let requiredVehicleOccupancy =
    currentEmployeesForRoute.length + (preliminaryGuardNeeded ? 1 : 0);

  const sortedFleet = [...(profile.fleet || [])].sort(
    (a, b) => a.capacity - b.capacity
  );
  let assignedVehicleConfig = null;

  if (sortedFleet.length > 0) {
    for (const vehicleOption of sortedFleet) {
      if (
        vehicleOption.capacity >= requiredVehicleOccupancy &&
        availableFleetCounts[vehicleOption.type] > 0
      ) {
        assignedVehicleConfig = vehicleOption;
        break;
      }
    }
  }

  if (assignedVehicleConfig) {
    availableFleetCounts[assignedVehicleConfig.type]--;
    routeShell.afterFleetExhaustion = false;
  } else {
    routeShell.afterFleetExhaustion = true;
    const mediumFallbackConfig = sortedFleet.find((v) => v.type === "m");

    if (!mediumFallbackConfig) {
      const errorMsg = `Fallback 'm' type vehicle not defined. Cannot route group.`;
      routeShell.error = true;
      routeShell.errorMessage = errorMsg;
      employeesTrimmedOff.push(...currentEmployeesForRoute);
      assignedVehicleConfig = { type: "NONE_M_MISSING", capacity: 0 };
    } else {
      assignedVehicleConfig = mediumFallbackConfig;
      if (assignedVehicleConfig.capacity < requiredVehicleOccupancy) {
        console.warn(
          `[Fleet] Route ${routeShell.uniqueKey} (Req Occupancy: ${requiredVehicleOccupancy}) assigned fallback 'm' (Cap: ${assignedVehicleConfig.capacity}) which is smaller. Trimming will occur.`
        );
      }
    }
  }

  routeShell.assignedVehicleType = assignedVehicleConfig.type;
  routeShell.vehicleCapacity = assignedVehicleConfig.capacity;

  if (routeShell.error) {
    routeShell.employees = [];
    return { employeesTrimmedOff };
  }

  let finalIsSpecial = false;
  let maxPassengersAllowedInVehicle =
    routeShell.vehicleCapacity - (routeShell.guardNeeded ? 1 : 0);

  let trimmingIteration = 0;
  const MAX_TRIMMING_ITERATIONS = currentEmployeesForRoute.length + 3;

  while (
    trimmingIteration++ < MAX_TRIMMING_ITERATIONS &&
    currentEmployeesForRoute.length > 0
  ) {
    finalIsSpecial = currentEmployeesForRoute.some(isSpecialNeedsUser);
    let currentMaxPassengers = maxPassengersAllowedInVehicle;

    if (finalIsSpecial) {
      currentMaxPassengers = Math.min(
        maxPassengersAllowedInVehicle,
        routeShell.guardNeeded ? 1 : 2
      );
    }
    currentMaxPassengers = Math.max(0, currentMaxPassengers);

    if (currentEmployeesForRoute.length > currentMaxPassengers) {
      const empToTrim = isDropoff
        ? currentEmployeesForRoute.shift()
        : currentEmployeesForRoute.pop();
      if (empToTrim) employeesTrimmedOff.push(empToTrim);

      // CORRECTED: Re-check guard needed after trimming
      if (activateGuardSystem && currentEmployeesForRoute.length > 0) {
        const critIdxRecheck = isDropoff
          ? currentEmployeesForRoute.length - 1
          : 0;
        const newGuardNeededStatus =
          currentEmployeesForRoute[critIdxRecheck]?.gender === "F";
        if (routeShell.guardNeeded !== newGuardNeededStatus) {
          routeShell.guardNeeded = newGuardNeededStatus;
          maxPassengersAllowedInVehicle =
            routeShell.vehicleCapacity - (routeShell.guardNeeded ? 1 : 0);
        }
      } else if (
        currentEmployeesForRoute.length === 0 &&
        routeShell.guardNeeded
      ) {
        routeShell.guardNeeded = false;
        maxPassengersAllowedInVehicle = routeShell.vehicleCapacity;
      }
    } else {
      break;
    }
  }
  if (trimmingIteration >= MAX_TRIMMING_ITERATIONS)
    console.warn(
      `[Fleet Trim] Max iterations reached for ${routeShell.uniqueKey}`
    );

  routeShell.employees = [...currentEmployeesForRoute];
  routeShell.isSpecialNeedsRoute =
    routeShell.employees.some(isSpecialNeedsUser);

  if (
    routeShell.employees.length === 0 &&
    preliminaryEmployeesInGroup.length > 0 &&
    !routeShell.error
  ) {
    routeShell.error = true;
    routeShell.errorMessage = `Route became empty after vehicle assignment and guard trimming (Vehicle: ${routeShell.assignedVehicleType})`;
  }
  if (employeesTrimmedOff.length > 0) {
    console.log(
      `[Fleet] Route ${routeShell.uniqueKey} (Type: ${routeShell.assignedVehicleType}, Final Emps: ${routeShell.employees.length}) trimmed ${employeesTrimmedOff.length} employees.`
    );
  }
  return { employeesTrimmedOff };
}

async function processEmployeeBatch(
  employees,
  targetGroupSizeForHeuristic,
  facility,
  tripType,
  maxDuration,
  pickupTimePerEmployee,
  activateGuardSystem,
  profile,
  availableFleetCounts,
  city,
  shiftTime
) {
  const routes = [];
  let employeesAddedToMasterUnroutedThisBatch = [];
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const facilityCoordinates = [facility.geoY, facility.geoX];
  const deferredForInitialOSRM = [];

  const validEmployees = employees.filter(
    (emp) => emp.location?.lat != null && emp.location?.lng != null
  );
  if (validEmployees.length === 0) {
    return { routes, employeesAddedToMasterUnrouted: [] };
  }

  let globalRemainingEmployees = [...validEmployees]
    .map((emp) => ({
      ...emp,
      distToFacility: haversineDistance(
        [emp.location.lat, emp.location.lng],
        [facility.geoY, facility.geoX]
      ),
      isMedical: emp.isMedical || false,
      isPWD: emp.isPWD || false,
    }))
    .sort((a, b) =>
      isDropoff
        ? a.distToFacility - b.distToFacility
        : b.distToFacility - a.distToFacility
    );

  let batchRouteCounter = 0;

  mainLoop: while (globalRemainingEmployees.length > 0) {
    batchRouteCounter++;
    let currentHeuristicRouteMaxOccupancy = targetGroupSizeForHeuristic;
    let routeIsCurrentlySpecialNeedsHeuristic = false;

    const firstEmployeeForThisRoute = globalRemainingEmployees.shift();
    if (!firstEmployeeForThisRoute) break;

    const firstEmpCoords = [
      firstEmployeeForThisRoute.location.lat,
      firstEmployeeForThisRoute.location.lng,
    ];
    const firstRouteCoordsOSRM = isDropoff
      ? [facilityCoordinates, firstEmpCoords]
      : [firstEmpCoords, facilityCoordinates];
    const firstRouteDetailsOSRM = await calculateRouteDetails(
      firstRouteCoordsOSRM,
      [firstEmployeeForThisRoute],
      pickupTimePerEmployee,
      tripType,
      city,
      shiftTime
    );

    if (firstRouteDetailsOSRM.error) {
      deferredForInitialOSRM.push(firstEmployeeForThisRoute);
      continue;
    }

    const tempHeuristicRouteForValidation = {
      employees: [firstEmployeeForThisRoute],
      routeDetails: firstRouteDetailsOSRM,
      uniqueKey: `temp_val_${batchRouteCounter}`,
    };
    if (
      !(await checkRouteDeviation(
        tempHeuristicRouteForValidation,
        facility,
        profile
      )) ||
      (maxDuration && firstRouteDetailsOSRM.totalDuration > maxDuration)
    ) {
      deferredForInitialOSRM.push(firstEmployeeForThisRoute);
      continue;
    }

    let preliminaryEmployeesForCurrentRoute = [firstEmployeeForThisRoute];
    if (isSpecialNeedsUser(firstEmployeeForThisRoute)) {
      routeIsCurrentlySpecialNeedsHeuristic = true;
      currentHeuristicRouteMaxOccupancy = 2;
    }

    let tempRemainingForThisHeuristicAttempt = globalRemainingEmployees.filter(
      (e) => e.empCode !== firstEmployeeForThisRoute.empCode
    );
    const MAX_NEXT_STOP_DISTANCE_KM_HEURISTIC = MAX_SWAP_DISTANCE_KM * 2.0;

    while (
      preliminaryEmployeesForCurrentRoute.length <
        currentHeuristicRouteMaxOccupancy &&
      tempRemainingForThisHeuristicAttempt.length > 0
    ) {
      const currentLastEmpInPrelim =
        preliminaryEmployeesForCurrentRoute[
          preliminaryEmployeesForCurrentRoute.length - 1
        ];
      let bestCandidate = null;
      let bestScore = -Infinity;
      let bestCandidateIndex = -1;

      tempRemainingForThisHeuristicAttempt.forEach((candidateEmp, idx) => {
        const candidateIsSpecial = isSpecialNeedsUser(candidateEmp);
        if (routeIsCurrentlySpecialNeedsHeuristic && !candidateIsSpecial)
          return;
        if (
          !routeIsCurrentlySpecialNeedsHeuristic &&
          candidateIsSpecial &&
          preliminaryEmployeesForCurrentRoute.length > 0 &&
          !isSpecialNeedsUser(preliminaryEmployeesForCurrentRoute[0])
        )
          return;

        const distToLast = haversineDistance(
          [
            currentLastEmpInPrelim.location.lat,
            currentLastEmpInPrelim.location.lng,
          ],
          [candidateEmp.location.lat, candidateEmp.location.lng]
        );
        if (distToLast > MAX_NEXT_STOP_DISTANCE_KM_HEURISTIC) return;
        const score = 1 / (1 + distToLast);
        if (score > bestScore) {
          bestScore = score;
          bestCandidate = candidateEmp;
          bestCandidateIndex = idx;
        }
      });

      if (!bestCandidate) break;
      const nextEmployeeToPick = bestCandidate;

      const tentativePrelimEmployees = [
        ...preliminaryEmployeesForCurrentRoute,
        nextEmployeeToPick,
      ];
      const tentativeCoords = tentativePrelimEmployees.map((emp) => [
        emp.location.lat,
        emp.location.lng,
      ]);
      const allTentativeCoords = isDropoff
        ? [facilityCoordinates, ...tentativeCoords]
        : [...tentativeCoords, facilityCoordinates];
      const tentativeDetails = await calculateRouteDetails(
        allTentativeCoords,
        tentativePrelimEmployees,
        pickupTimePerEmployee,
        tripType,
        city,
        shiftTime
      );

      if (tentativeDetails.error) {
        tempRemainingForThisHeuristicAttempt.splice(bestCandidateIndex, 1);
        continue;
      }
      const tempRouteForValidation = {
        employees: tentativePrelimEmployees,
        routeDetails: tentativeDetails,
        uniqueKey: `temp_val_add_${batchRouteCounter}`,
      };
      if (
        !(await checkRouteDeviation(
          tempRouteForValidation,
          facility,
          profile
        )) ||
        (maxDuration && tentativeDetails.totalDuration > maxDuration)
      ) {
        tempRemainingForThisHeuristicAttempt.splice(bestCandidateIndex, 1);
        continue;
      }

      preliminaryEmployeesForCurrentRoute.push(nextEmployeeToPick);
      tempRemainingForThisHeuristicAttempt.splice(bestCandidateIndex, 1);

      if (
        isSpecialNeedsUser(nextEmployeeToPick) &&
        !routeIsCurrentlySpecialNeedsHeuristic
      ) {
        routeIsCurrentlySpecialNeedsHeuristic = true;
        currentHeuristicRouteMaxOccupancy = 2;
      }
    }

    const routeShellForVehicleAssignment = {
      zone: firstEmployeeForThisRoute.zone,
      tripType: tripType,
      uniqueKey: `${
        firstEmployeeForThisRoute.zone
      }_batch_${batchRouteCounter}_${uuidv4()}`,
    };

    const { employeesTrimmedOff } = await assignVehicleAndFinalizeGroup(
      routeShellForVehicleAssignment,
      preliminaryEmployeesForCurrentRoute,
      profile,
      availableFleetCounts,
      shiftTime,
      tripType,
      activateGuardSystem
    );

    if (employeesTrimmedOff.length > 0) {
      employeesAddedToMasterUnroutedThisBatch.push(...employeesTrimmedOff);
    }

    if (
      routeShellForVehicleAssignment.error ||
      routeShellForVehicleAssignment.employees.length === 0
    ) {
      // Handled
    } else {
      const finalRouteCoords = routeShellForVehicleAssignment.employees.map(
        (emp) => [emp.location.lat, emp.location.lng]
      );
      const finalAllCoords = isDropoff
        ? [facilityCoordinates, ...finalRouteCoords]
        : [...finalRouteCoords, facilityCoordinates];
      const finalRouteDetailsOSRM = await calculateRouteDetails(
        finalAllCoords,
        routeShellForVehicleAssignment.employees,
        pickupTimePerEmployee,
        tripType,
        city,
        shiftTime
      );

      if (finalRouteDetailsOSRM.error) {
        employeesAddedToMasterUnroutedThisBatch.push(
          ...routeShellForVehicleAssignment.employees
        );
      } else {
        updateRouteWithDetails(
          routeShellForVehicleAssignment,
          finalRouteDetailsOSRM
        );
        if (
          !(await checkRouteDeviation(
            routeShellForVehicleAssignment,
            facility,
            profile
          )) ||
          (maxDuration &&
            routeShellForVehicleAssignment.routeDetails.totalDuration >
              maxDuration)
        ) {
          employeesAddedToMasterUnroutedThisBatch.push(
            ...routeShellForVehicleAssignment.employees
          );
        } else {
          routes.push(routeShellForVehicleAssignment);
        }
      }
    }
    const processedEmpCodesThisIteration = new Set([
      ...(routeShellForVehicleAssignment.employees?.map((e) => e.empCode) ||
        []),
      ...employeesTrimmedOff.map((e) => e.empCode),
    ]);
    globalRemainingEmployees = globalRemainingEmployees.filter(
      (emp) => !processedEmpCodesThisIteration.has(emp.empCode)
    );
  }

  if (deferredForInitialOSRM.length > 0) {
    employeesAddedToMasterUnroutedThisBatch.push(...deferredForInitialOSRM);
  }
  return {
    routes,
    employeesAddedToMasterUnrouted: employeesAddedToMasterUnroutedThisBatch,
  };
}

async function reOptimizeSwappedRouteWithORTools(
  routeToReOptimize,
  facilityData,
  pickupTimePerEmployee,
  city
) {
  const {
    employees: swappedEmployees,
    tripType,
    zone,
    vehicleCapacity,
  } = routeToReOptimize;
  const profileMaxDuration = facilityData.profile?.maxDuration || 7200;

  if (!swappedEmployees || swappedEmployees.length === 0) {
    return {
      reOptimized: false,
      employees: swappedEmployees,
      error: "No employees for re-optimization",
    };
  }

  const facilityLocation = { lat: facilityData.geoY, lng: facilityData.geoX };
  let pinnedEmployee,
    otherEmployeesInRoute,
    fixedNodeParam = {};

  if (tripType.toLowerCase() === "pickup") {
    pinnedEmployee = swappedEmployees[0];
    otherEmployeesInRoute = swappedEmployees.slice(1);
    fixedNodeParam = { fixed_start_node_index_in_matrix: 1 };
  } else {
    pinnedEmployee = swappedEmployees[swappedEmployees.length - 1];
    otherEmployeesInRoute = swappedEmployees.slice(0, -1);
    fixedNodeParam = { fixed_end_node_index_in_matrix: 1 };
  }

  if (!pinnedEmployee)
    return {
      reOptimized: false,
      employees: swappedEmployees,
      error: "Could not identify pinned employee",
    };

  const employeesForThisOrRun = [pinnedEmployee, ...otherEmployeesInRoute];
  try {
    const matrixData = await generateDistanceDurationMatrix(
      employeesForThisOrRun,
      facilityLocation,
      city
    );
    const { distanceMatrix, durationMatrix } = matrixData;
    if (
      !distanceMatrix ||
      distanceMatrix.length === 0 ||
      (distanceMatrix.length > 0 && distanceMatrix[0].length === 0)
    ) {
      return {
        reOptimized: false,
        employees: swappedEmployees,
        error: "Empty distance matrix",
      };
    }
    const pointMapForReSolve = matrixData.pointMap;
    if (pointMapForReSolve.length !== distanceMatrix.length) {
      return {
        reOptimized: false,
        employees: swappedEmployees,
        error: "Matrix-PointMap mismatch",
      };
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

    const pythonExecutable = process.env.PYTHON_EXECUTABLE || "python";
    const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py");
    if (!fs.existsSync(scriptPath))
      throw new Error(`Solver script not found: ${scriptPath}`);

    const pythonProcess = spawn(pythonExecutable, [scriptPath]);
    let scriptOutput = "";
    let scriptError = "";
    pythonProcess.stdin.write(JSON.stringify(orToolsInput));
    pythonProcess.stdin.end();
    pythonProcess.stdout.on(
      "data",
      (data) => (scriptOutput += data.toString())
    );
    pythonProcess.stderr.on("data", (data) => (scriptError += data.toString()));

    return new Promise((resolve) => {
      pythonProcess.on("close", (code) => {
        if (code !== 0) {
          console.error(
            `[RE-OPTIMIZE Python stderr FOR ZONE ${zone}]: ${scriptError}`
          );
          return resolve({
            reOptimized: false,
            employees: swappedEmployees,
            error: `Python exit ${code}: ${scriptError.substring(0, 100)}`,
          });
        }
        try {
          let solution = null;
          const lines = scriptOutput.trim().split("\n");
          for (let i = lines.length - 1; i >= 0; i--) {
            try {
              solution = JSON.parse(lines[i]);
              if (typeof solution === "object" && solution !== null) break;
            } catch (e) {
              /* not this line */
            }
          }
          if (!solution)
            throw new Error("No valid JSON solution from Python for re-opt.");

          if (
            solution.error ||
            (solution.dropped_node_indices &&
              solution.dropped_node_indices.length > 0)
          ) {
            return resolve({
              reOptimized: false,
              employees: swappedEmployees,
              error: solution.error || "Nodes dropped in re-opt",
            });
          }
          if (solution.routes?.[0]?.length > 0) {
            const routeNodeIndices = solution.routes[0];
            const reOptimizedEmployeeList = routeNodeIndices
              .map((nodeIndex) =>
                nodeIndex === 0 || nodeIndex >= pointMapForReSolve.length
                  ? null
                  : pointMapForReSolve[nodeIndex]
              )
              .filter((emp) => emp != null && !emp.isFacility);
            resolve({ reOptimized: true, employees: reOptimizedEmployeeList });
          } else {
            resolve({
              reOptimized: false,
              employees: swappedEmployees,
              error: "No route from re-opt",
            });
          }
        } catch (e) {
          resolve({
            reOptimized: false,
            employees: swappedEmployees,
            error: `Parse error re-opt: ${e.message}`,
          });
        }
      });
      pythonProcess.on("error", (err) =>
        resolve({
          reOptimized: false,
          employees: swappedEmployees,
          error: `Python spawn error re-opt: ${err.message}`,
        })
      );
    });
  } catch (error) {
    console.error(`[RE-OPTIMIZE] Critical error for zone "${zone}":`, error);
    return {
      reOptimized: false,
      employees: swappedEmployees,
      error: error.message,
    };
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
  forceSingleVehicleOptimization = false,
  city
) {
  const currentZoneNameForLogging =
    zoneName || zoneEmployees[0]?.zone || "UNKNOWN_ZONE_IN_SOLVER";
  if (!zoneEmployees || zoneEmployees.length === 0) {
    return { routes: [], droppedEmployees: [] };
  }

  const facilityLocation = { lat: facilityData.geoY, lng: facilityData.geoX };
  try {
    const matrixData = await generateDistanceDurationMatrix(
      zoneEmployees,
      facilityLocation,
      city
    );
    const {
      distanceMatrix,
      durationMatrix,
      pointMap: pointMapForCurrentZone,
    } = matrixData;

    if (
      !distanceMatrix ||
      distanceMatrix.length === 0 ||
      (distanceMatrix.length > 0 && distanceMatrix[0].length === 0)
    ) {
      return {
        routes: [],
        droppedEmployees: zoneEmployees,
        error: "Empty distance matrix for OR-Tools",
      };
    }
    if (pointMapForCurrentZone.length !== distanceMatrix.length) {
      return {
        routes: [],
        droppedEmployees: zoneEmployees,
        error: "Matrix-PointMap mismatch for OR-Tools",
      };
    }

    const numCustomers = zoneEmployees.length;
    const numVehiclesForSolver = forceSingleVehicleOptimization
      ? 1
      : numCustomers > 0
      ? numCustomers
      : 1;

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
        facilityData.profile?.allowDroppingVisitsForProblematicZones !== false,
      drop_visit_penalty: facilityData.profile?.dropPenalty || 360000,
      facility_coords: [facilityLocation.lat, facilityLocation.lng],
      trip_type: tripType.toUpperCase(),
      direction_penalty_weight:
        facilityData.profile?.directionPenaltyWeight || 2.0,
    };

    const pythonExecutable = process.env.PYTHON_EXECUTABLE || "python";
    const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py");
    if (!fs.existsSync(scriptPath))
      throw new Error(`Solver script not found: ${scriptPath}`);

    const pythonProcess = spawn(pythonExecutable, [scriptPath]);
    let scriptOutput = "";
    let scriptError = "";
    pythonProcess.stdin.write(JSON.stringify(orToolsInput));
    pythonProcess.stdin.end();
    pythonProcess.stdout.on(
      "data",
      (data) => (scriptOutput += data.toString())
    );
    pythonProcess.stderr.on("data", (data) => (scriptError += data.toString()));

    return new Promise((resolve, reject) => {
      pythonProcess.on("close", (code) => {
        if (code !== 0) {
          console.error(
            `[OR-TOOLS Python stderr FOR ZONE ${currentZoneNameForLogging}]: ${scriptError}`
          );
          return reject(
            new Error(
              `Python script (zone "${currentZoneNameForLogging}") exit ${code}. Stderr: ${scriptError.substring(
                0,
                200
              )}`
            )
          );
        }
        try {
          let solution = null;
          const lines = scriptOutput.trim().split("\n");
          for (let i = lines.length - 1; i >= 0; i--) {
            try {
              solution = JSON.parse(lines[i]);
              if (typeof solution === "object" && solution !== null) break;
            } catch (e) {
              /* not this line */
            }
          }
          if (!solution)
            throw new Error(
              "No valid JSON solution found in Python output for OR-Tools."
            );

          if (solution.error)
            return reject(
              new Error(`OR-Tools solver error: ${solution.error}`)
            );

          const orRoutes = [];
          const solutionDroppedIndices = solution.dropped_node_indices || [];
          const droppedEmployees = solutionDroppedIndices
            .map((nodeIdx) =>
              nodeIdx > 0 && nodeIdx < pointMapForCurrentZone.length
                ? pointMapForCurrentZone[nodeIdx]
                : null
            )
            .filter(Boolean);

          if (solution.routes && Array.isArray(solution.routes)) {
            solution.routes.forEach((routeNodeIndices) => {
              if (routeNodeIndices.length > 0) {
                const currentRouteEmployees = routeNodeIndices
                  .map((nodeIndex) =>
                    nodeIndex === 0 ||
                    nodeIndex >= pointMapForCurrentZone.length
                      ? null
                      : pointMapForCurrentZone[nodeIndex]
                  )
                  .filter((emp) => emp != null && !emp.isFacility);
                if (currentRouteEmployees.length > 0) {
                  orRoutes.push({
                    employees: currentRouteEmployees,
                    vehicleCapacity,
                    zone: currentZoneNameForLogging,
                    tripType,
                  });
                }
              }
            });
          }
          resolve({ routes: orRoutes, droppedEmployees });
        } catch (e) {
          console.error(
            `[OR-TOOLS SOLVER] Error parsing Python stdout for zone "${currentZoneNameForLogging}":`,
            e,
            "\nRaw stdout:\n",
            scriptOutput
          );
          reject(
            new Error(
              `Failed to parse OR-Tools solution: ${
                e.message
              }. Output: ${scriptOutput.substring(0, 500)}`
            )
          );
        }
      });
      pythonProcess.on("error", (err) =>
        reject(new Error(`Failed to start Python subprocess: ${err.message}`))
      );
    });
  } catch (error) {
    console.error(
      `[OR-TOOLS SOLVER] Critical error for zone "${currentZoneNameForLogging}":`,
      error
    );
    return {
      routes: [],
      droppedEmployees: [...zoneEmployees],
      error: error.message,
    };
  }
}

// Add these constants at the top of your file
const MAX_UNROUTED_PROCESSING_ATTEMPTS = 5; // Maximum attempts per employee
const UNROUTED_DEVIATION_TOLERANCE = 0.15; // 15% tolerance for unrouted employees
const FORCE_SINGLETON_DISTANCE_THRESHOLD = 20; // Force singleton if > 20km from facility

// Add this function to track processing attempts
function createUnroutedAttemptTracker() {
  const attempts = new Map();

  return {
    getAttempts: (empCode) => attempts.get(empCode) || 0,
    incrementAttempts: (empCode) => {
      const current = attempts.get(empCode) || 0;
      attempts.set(empCode, current + 1);
      return current + 1;
    },
    hasExceededMaxAttempts: (empCode) => {
      return (attempts.get(empCode) || 0) >= MAX_UNROUTED_PROCESSING_ATTEMPTS;
    },
  };
}

// Enhanced route deviation check for unrouted employees
// Simplified and stricter route deviation check for unrouted employees
async function checkRouteDeviationForUnrouted(
  route,
  facility,
  profile,
  isUnroutedPass = false
) {

   if (BYPASS_ROUTE_DEVIATION_CHECKS) {
    console.log(`[RouteDeviation] BYPASSING deviation check for route ${route.uniqueKey || route.routeNumber} (global override).`);
    return true;
  }

  // Always run the standard deviation check first
  const baseCheck = await checkRouteDeviation(route, facility, profile);

  if (baseCheck) {
    return true; // Passes normal deviation check - route is fine
  }

  // If unrouted pass, apply MINIMAL tolerance only for borderline cases
  if (isUnroutedPass && profile?.routeDeviationRules) {
    const city = profile?.name;
    const fastApiCity = getFastApiCityKey(city);

    if (!route?.employees || route.employees.length === 0) return true;

    const ruleKeys = Object.keys(profile.routeDeviationRules);
    if (ruleKeys.length === 0) return true;

    let effectiveRuleKey =
      profile.facilityType && profile.routeDeviationRules[profile.facilityType]
        ? profile.facilityType
        : ruleKeys[0];
    let rules = profile.routeDeviationRules[effectiveRuleKey];

    if (!rules || !Array.isArray(rules) || rules.length === 0) return true;

    // Calculate farthest employee distance (same as base check)
    let farthestEmployeeDistanceKm = 0;
    const isDropoffRoute = route.tripType?.toLowerCase() === "dropoff";

    if (route.employees.length > 0) {
      const farthestEmployee = isDropoffRoute
        ? route.employees[route.employees.length - 1]
        : route.employees[0];

      if (
        farthestEmployee?.location?.lng != null &&
        farthestEmployee?.location?.lat != null
      ) {
        try {
          // Prepare coordinates as [lng, lat] pairs for FastAPI
          const coords = isDropoffRoute
            ? [
                [facility.geoX, facility.geoY],
                [farthestEmployee.location.lng, farthestEmployee.location.lat],
              ]
            : [
                [farthestEmployee.location.lng, farthestEmployee.location.lat],
                [facility.geoX, facility.geoY],
              ];
          const response = await fetchApi(`${FASTAPI_GATEWAY_URL}/route`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              city: fastApiCity,
              coordinates: coords,
              overview: "false",
              steps: false,
              geometries: "polyline",
            }),
            timeout: OSRM_PROBE_TIMEOUT,
          });
          if (response.ok) {
            const data = await response.json();
            if (data.code === "Ok" && data.routes?.[0]?.distance != null) {
              farthestEmployeeDistanceKm = data.routes[0].distance / 1000;
            }
          }
        } catch (err) {
          console.warn(`[UnroutedDeviation] OSRM error: ${err.message}`);
          return false; // If we can't calculate, don't allow it
        }
      }
    }

    // Find applicable rule (same logic as base check)
    const EPSILON = 0.001;
    let applicableRule = rules.find(
      (rule) =>
        farthestEmployeeDistanceKm >= rule.minDistKm - EPSILON &&
        farthestEmployeeDistanceKm <= rule.maxDistKm + EPSILON
    );

    if (!applicableRule) {
      const sortedRules = [...rules].sort((a, b) => a.maxDistKm - b.maxDistKm);
      if (
        sortedRules.length > 0 &&
        farthestEmployeeDistanceKm >
          sortedRules[sortedRules.length - 1].maxDistKm
      ) {
        applicableRule = sortedRules[sortedRules.length - 1];
      } else if (sortedRules.length > 0) {
        let closestRule = sortedRules[0];
        for (const rule of sortedRules) {
          if (farthestEmployeeDistanceKm <= rule.maxDistKm + EPSILON) {
            closestRule = rule;
            break;
          }
        }
        applicableRule = closestRule;
      }
    }

    if (!applicableRule?.maxTotalOneWayKm) return false;

    const relevantRouteDistanceKm =
      (route.routeDetails.totalDistance || 0) / 1000;
    const ruleLimit = applicableRule.maxTotalOneWayKm;
    const exceedanceKm = relevantRouteDistanceKm - ruleLimit;
    const exceedancePercentage = exceedanceKm / ruleLimit;

    // ONLY allow very small exceedances for unrouted (< 5% or < 2km, whichever is smaller)
    const maxAllowedExceedanceKm = Math.min(ruleLimit * 0.05, 2.0);

    if (exceedanceKm <= maxAllowedExceedanceKm) {
      console.log(
        `[UnroutedDeviation] Route ${
          route.uniqueKey || route.routeNumber
        }: PASSED with minimal tolerance. Rule limit: ${ruleLimit}km, Actual: ${relevantRouteDistanceKm.toFixed(
          2
        )}km (exceeds by ${exceedanceKm.toFixed(
          2
        )}km). FarthestEmpDist: ${farthestEmployeeDistanceKm.toFixed(2)}km.`
      );
      return true;
    }

    console.warn(
      `[UnroutedDeviation] Route ${
        route.uniqueKey || route.routeNumber
      }: REJECTED. Rule limit: ${ruleLimit}km, Actual: ${relevantRouteDistanceKm.toFixed(
        2
      )}km (exceeds by ${exceedanceKm.toFixed(2)}km, ${(
        exceedancePercentage * 100
      ).toFixed(1)}%). Max allowed exceedance: ${maxAllowedExceedanceKm.toFixed(
        2
      )}km. FarthestEmpDist: ${farthestEmployeeDistanceKm.toFixed(2)}km.`
    );
  }

  return false;
}

// Remove the distance-based pre-filtering and keep it simple
function preFilterEmployeesForProcessing(employees, facility) {
  // Only filter out employees that are clearly impossible to reach
  const impossibleEmployees = [];
  const routeableEmployees = [];

  const IMPOSSIBLE_DISTANCE_THRESHOLD = 50; // Only filter truly unreachable employees

  for (const emp of employees) {
    const distToFacility = haversineDistance(
      [emp.location.lat, emp.location.lng],
      [facility.geoY, facility.geoX]
    );

    if (distToFacility > IMPOSSIBLE_DISTANCE_THRESHOLD) {
      impossibleEmployees.push(emp);
      console.warn(
        `[Pre-filter] Employee ${emp.empCode} is ${distToFacility.toFixed(
          2
        )}km from facility. Impossible to route.`
      );
    } else {
      routeableEmployees.push(emp);
    }
  }

  return { routeableEmployees, impossibleEmployees };
}

// Simplified unrouted processing that respects deviation rules strictly
async function processUnroutedEmployeesWithSafeguards(
  finalUnroutedForProcessing,
  profile,
  availableFleetCounts,
  facility,
  tripType,
  profileMaxDuration,
  pickupTimePerEmployee,
  activateGuardSystemFromInput,
  shiftTime,
  city,
  totalRouteCount,
  routeDataContainer,
  reportingTime = 0  // <-- ADD THIS PARAMETER
) {
  if (finalUnroutedForProcessing.length === 0) {
    return {
      processedRoutes: [],
      remainingUnrouted: [],
      updatedRouteCount: totalRouteCount,
    };
  }

  console.log(
    `\n[UNROUTED HANDLING] Processing ${finalUnroutedForProcessing.length} unrouted employees with strict deviation checks...`
  );

  const attemptTracker = createUnroutedAttemptTracker();
  const processedRoutes = [];
  let currentRouteCount = totalRouteCount;

  // Distance-based constants for unrouted processing
  const UNROUTED_FORCE_SINGLETON_DISTANCE = 40; // Force singleton if > 20km from facility
  const UNROUTED_MAX_GROUP_DISTANCE = 5.0; // Max distance between any two employees in unrouted group
  const UNROUTED_MAX_CONSECUTIVE_DISTANCE = 5.0; // Max distance between consecutive employees
  const UNROUTED_MAX_GROUP_SPAN = 12; // Max distance between farthest employees in a group

  let MAX_INITIAL_GROUP_SIZE_UNROUTED = profile.unroutedGroupSizeTarget || 2; // Reduced from 3
  const MAX_TRIM_ATTEMPTS_PER_GROUP = profile.unroutedTrimAttempts || 2;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const facilityCoordinates = [facility.geoY, facility.geoX];

  // Helper function to validate consecutive distances
  function validateUnroutedGroupDistances(
    employees,
    maxConsecutiveKm = UNROUTED_MAX_CONSECUTIVE_DISTANCE
  ) {
    if (!employees || employees.length < 2) return true;

    for (let i = 0; i < employees.length - 1; i++) {
      const emp1 = employees[i];
      const emp2 = employees[i + 1];

      const distance = haversineDistance(
        [emp1.location.lat, emp1.location.lng],
        [emp2.location.lat, emp2.location.lng]
      );

      if (distance > maxConsecutiveKm) {
        console.warn(
          `[Unrouted Consecutive] Employees ${emp1.empCode} and ${
            emp2.empCode
          } are ${distance.toFixed(2)}km apart (max: ${maxConsecutiveKm}km)`
        );
        return false;
      }
    }
    return true;
  }

  // Helper function to check maximum span in a group
  function checkGroupSpan(employees, maxSpanKm = UNROUTED_MAX_GROUP_SPAN) {
    if (!employees || employees.length < 2) return true;

    let maxDistance = 0;
    for (let i = 0; i < employees.length; i++) {
      for (let j = i + 1; j < employees.length; j++) {
        const dist = haversineDistance(
          [employees[i].location.lat, employees[i].location.lng],
          [employees[j].location.lat, employees[j].location.lng]
        );
        maxDistance = Math.max(maxDistance, dist);
      }
    }

    if (maxDistance > maxSpanKm) {
      console.warn(
        `[Unrouted Group Span] Max distance between employees in group is ${maxDistance.toFixed(
          2
        )}km (max: ${maxSpanKm}km)`
      );
      return false;
    }
    return true;
  }

  // Simple pre-filtering - only remove truly impossible employees
  const { routeableEmployees, impossibleEmployees } =
    preFilterEmployeesForProcessing(finalUnroutedForProcessing, facility);

  // Separate employees by distance from facility for singleton vs grouping decision
  const singletonCandidates = [];
  const groupableCandidates = [];

  for (const emp of routeableEmployees) {
    const distToFacility = haversineDistance(
      [emp.location.lat, emp.location.lng],
      [facility.geoY, facility.geoX]
    );

    if (distToFacility > UNROUTED_FORCE_SINGLETON_DISTANCE) {
      console.log(
        `[Unrouted Singleton] Employee ${
          emp.empCode
        } is ${distToFacility.toFixed(2)}km from facility. Forcing singleton.`
      );
      singletonCandidates.push(emp);
    } else {
      groupableCandidates.push(emp);
    }
  }

  // Process forced singletons first
  for (const emp of singletonCandidates) {
    currentRouteCount++;

    const singletonRoute = {
      routeNumber: currentRouteCount,
      employees: [emp],
      zone: emp.zone || "UNROUTED_SINGLETON",
      tripType: tripType,
      uniqueKey: `${
        emp.zone
      }_UNROUTED_SINGLETON_${currentRouteCount}_${uuidv4()}`,
    };

    // Vehicle assignment for singleton
    const { employeesTrimmedOff } = await assignVehicleAndFinalizeGroup(
      singletonRoute,
      [emp],
      profile,
      availableFleetCounts,
      shiftTime,
      tripType,
      activateGuardSystemFromInput
    );

    if (!singletonRoute.error && singletonRoute.employees.length > 0) {
      // Calculate route details
      const singletonCoords = [emp.location.lat, emp.location.lng];
      const allCoords = isDropoff
        ? [facilityCoordinates, singletonCoords]
        : [singletonCoords, facilityCoordinates];
      const routeDetails = await calculateRouteDetails(
        allCoords,
        [emp],
        pickupTimePerEmployee,
        tripType,
        city,
        shiftTime
      );

      if (!routeDetails.error) {
        updateRouteWithDetails(singletonRoute, routeDetails);

        // More lenient deviation check for forced singletons
        if (
          await checkRouteDeviationForUnrouted(
            singletonRoute,
            facility,
            profile,
            true
          )
        ) {
          calculatePickupTimes(
            singletonRoute,
            shiftTime,
            pickupTimePerEmployee,
            reportingTime
          );
          singletonRoute.guardNeeded =
            activateGuardSystemFromInput && emp.gender === "F";
          processedRoutes.push(singletonRoute);
          console.log(
            `[Unrouted Singleton Success] Created singleton route ${singletonRoute.uniqueKey} for distant employee ${emp.empCode}`
          );
        } else {
          impossibleEmployees.push(emp);
        }
      } else {
        impossibleEmployees.push(emp);
      }
    } else {
      impossibleEmployees.push(emp);
    }
  }

  // Now process groupable candidates with distance validation
  let remainingToRouteIteratively = [...groupableCandidates];
  const processedInThisUnroutedPass = new Set();
  let globalIterationCount = 0;
  const MAX_GLOBAL_ITERATIONS = groupableCandidates.length * 3; // Circuit breaker

  // Process employees with strict deviation adherence and distance validation
  while (
    remainingToRouteIteratively.length > 0 &&
    globalIterationCount < MAX_GLOBAL_ITERATIONS
  ) {
    globalIterationCount++;
    currentRouteCount++;

    // Filter out employees that have exceeded max attempts
    remainingToRouteIteratively = remainingToRouteIteratively.filter((emp) => {
      if (attemptTracker.hasExceededMaxAttempts(emp.empCode)) {
        console.warn(
          `[Unrouted Circuit Breaker] Employee ${emp.empCode} has exceeded ${MAX_UNROUTED_PROCESSING_ATTEMPTS} attempts. Moving to impossible list.`
        );
        impossibleEmployees.push(emp);
        return false;
      }
      return true;
    });

    if (remainingToRouteIteratively.length === 0) break;

    let initialGroupForThisAttempt = [];
    let tempHoldingForNextIteration = [];

    // Build initial group WITH DISTANCE VALIDATION
    let count = 0;
    while (
      remainingToRouteIteratively.length > 0 &&
      count < MAX_INITIAL_GROUP_SIZE_UNROUTED
    ) {
      const candidateEmp = remainingToRouteIteratively.shift();
      if (!processedInThisUnroutedPass.has(candidateEmp.empCode)) {
        // Check distance to existing employees in group
        let canAddToGroup = true;
        if (initialGroupForThisAttempt.length > 0) {
          for (const existingEmp of initialGroupForThisAttempt) {
            const distance = haversineDistance(
              [candidateEmp.location.lat, candidateEmp.location.lng],
              [existingEmp.location.lat, existingEmp.location.lng]
            );

            if (distance > UNROUTED_MAX_GROUP_DISTANCE) {
              console.log(
                `[Unrouted Distance Check] Employee ${
                  candidateEmp.empCode
                } is ${distance.toFixed(2)}km from ${
                  existingEmp.empCode
                }. Too far for grouping.`
              );
              canAddToGroup = false;
              break;
            }
          }
        }

        if (canAddToGroup) {
          initialGroupForThisAttempt.push(candidateEmp);
          processedInThisUnroutedPass.add(candidateEmp.empCode);
          attemptTracker.incrementAttempts(candidateEmp.empCode);
          count++;
        } else {
          // Put back for singleton processing
          tempHoldingForNextIteration.push(candidateEmp);
        }
      } else {
        tempHoldingForNextIteration.push(candidateEmp);
      }
    }
    remainingToRouteIteratively.unshift(...tempHoldingForNextIteration);

    if (initialGroupForThisAttempt.length === 0) break;

    // Adjust group size based on average distance from facility
    if (initialGroupForThisAttempt.length > 0) {
      const avgDistanceFromFacility =
        initialGroupForThisAttempt.reduce((sum, emp) => {
          return (
            sum +
            haversineDistance(
              [emp.location.lat, emp.location.lng],
              [facility.geoY, facility.geoX]
            )
          );
        }, 0) / initialGroupForThisAttempt.length;

      // If employees are far from facility, prefer smaller groups
      if (
        avgDistanceFromFacility > 15.0 &&
        initialGroupForThisAttempt.length > 1
      ) {
        console.log(
          `[Unrouted] Employees are ${avgDistanceFromFacility.toFixed(
            2
          )}km from facility on average. Reducing group size.`
        );
        // Remove excess employees and put them back
        while (initialGroupForThisAttempt.length > 1) {
          const removedEmp = initialGroupForThisAttempt.pop();
          processedInThisUnroutedPass.delete(removedEmp.empCode);
          remainingToRouteIteratively.unshift(removedEmp);
        }
      }
    }

    // Check group span before proceeding
    if (
      initialGroupForThisAttempt.length > 1 &&
      !checkGroupSpan(initialGroupForThisAttempt)
    ) {
      console.log(`[Unrouted] Group span too large. Breaking into singletons.`);

      // Process each as singleton instead
      for (const emp of initialGroupForThisAttempt) {
        processedInThisUnroutedPass.delete(emp.empCode);
        remainingToRouteIteratively.unshift(emp);
      }
      continue; // Skip to next iteration
    }

    let currentEmployeesInRouteAttempt = [...initialGroupForThisAttempt];
    let successfullyRoutedThisIteration = false;

    // Try different group sizes, trimming down if deviation fails
    for (
      let trimAttempt = 0;
      trimAttempt <= MAX_TRIM_ATTEMPTS_PER_GROUP;
      trimAttempt++
    ) {
      if (currentEmployeesInRouteAttempt.length === 0) break;

      // Validate consecutive distances before proceeding
      if (
        currentEmployeesInRouteAttempt.length > 1 &&
        !validateUnroutedGroupDistances(currentEmployeesInRouteAttempt)
      ) {
        console.log(
          `[Unrouted] Breaking group into singletons due to excessive consecutive distances`
        );

        // Process each employee as singleton
        for (const emp of currentEmployeesInRouteAttempt) {
          processedInThisUnroutedPass.delete(emp.empCode);
          remainingToRouteIteratively.unshift(emp);
        }
        break; // Skip to next iteration
      }

      const routeForThisAttempt = {
        routeNumber: currentRouteCount,
        employees: [...currentEmployeesInRouteAttempt],
        zone: currentEmployeesInRouteAttempt[0].zone || "UNROUTED_ITERATIVE",
        tripType: tripType,
        uniqueKey: `${
          currentEmployeesInRouteAttempt[0].zone
        }_UNROUTED_ITER_${currentRouteCount}_${trimAttempt}_${uuidv4()}`,
      };

      // Vehicle assignment
      const { employeesTrimmedOff: trimmedForCapacity } =
        await assignVehicleAndFinalizeGroup(
          routeForThisAttempt,
          [...currentEmployeesInRouteAttempt],
          profile,
          availableFleetCounts,
          shiftTime,
          tripType,
          activateGuardSystemFromInput
        );

      if (trimmedForCapacity.length > 0) {
        trimmedForCapacity.forEach((emp) => {
          processedInThisUnroutedPass.delete(emp.empCode);
        });
        remainingToRouteIteratively.unshift(...trimmedForCapacity);
        currentEmployeesInRouteAttempt = routeForThisAttempt.employees;
        if (currentEmployeesInRouteAttempt.length === 0) {
          break;
        }
      }

      if (
        routeForThisAttempt.error ||
        routeForThisAttempt.employees.length === 0
      ) {
        currentEmployeesInRouteAttempt.forEach((e) => {
          processedInThisUnroutedPass.delete(e.empCode);
          remainingToRouteIteratively.unshift(e);
        });
        break;
      }

      // OR-Tools optimization
      if (routeForThisAttempt.employees.length > 0) {
        try {
          const {
            routes: orRoutes,
            droppedEmployees: orDropped,
            error: orError,
          } = await solveZoneWithORTools(
            routeForThisAttempt.employees,
            facility,
            routeForThisAttempt.vehicleCapacity,
            profileMaxDuration,
            pickupTimePerEmployee,
            tripType,
            routeForThisAttempt.zone,
            true,
            city
          );

          if (orError) throw new Error(orError);

          if (orDropped?.length > 0) {
            orDropped.forEach((emp) => {
              processedInThisUnroutedPass.delete(emp.empCode);
              remainingToRouteIteratively.unshift(emp);
            });
          }

          if (orRoutes?.[0]?.employees.length > 0) {
            routeForThisAttempt.employees = orRoutes[0].employees;

            // Re-validate distances after OR-Tools optimization
            if (
              routeForThisAttempt.employees.length > 1 &&
              !validateUnroutedGroupDistances(routeForThisAttempt.employees)
            ) {
              console.warn(
                `[Unrouted] OR-Tools result failed distance validation. Breaking into singletons.`
              );
              routeForThisAttempt.employees.forEach((emp) => {
                processedInThisUnroutedPass.delete(emp.empCode);
                remainingToRouteIteratively.unshift(emp);
              });
              break;
            }
          }
        } catch (e) {
          console.warn(
            `OR-Tools failed for unrouted group ${routeForThisAttempt.uniqueKey}: ${e.message}`
          );
        }
      }

      if (routeForThisAttempt.employees.length === 0) {
        currentEmployeesInRouteAttempt.forEach((e) => {
          processedInThisUnroutedPass.delete(e.empCode);
          remainingToRouteIteratively.unshift(e);
        });
        break;
      }

      // Route calculation
      const routeCoords = routeForThisAttempt.employees.map((emp) => [
        emp.location.lat,
        emp.location.lng,
      ]);
      const allCoords = isDropoff
        ? [facilityCoordinates, ...routeCoords]
        : [...routeCoords, facilityCoordinates];
      const routeDetails = await calculateRouteDetails(
        allCoords,
        routeForThisAttempt.employees,
        pickupTimePerEmployee,
        tripType,
        city,
        shiftTime
      );

      if (routeDetails.error) {
        assignErrorState(
          routeForThisAttempt,
          `OSRM failed for unrouted: ${routeDetails.error}`
        );
        currentEmployeesInRouteAttempt.forEach((e) => {
          processedInThisUnroutedPass.delete(e.empCode);
          remainingToRouteIteratively.unshift(e);
        });
        break;
      }

      updateRouteWithDetails(routeForThisAttempt, routeDetails);

      // Guard handling
      let unroutedRouteModifiedBySwap = false;
      if (
        activateGuardSystemFromInput &&
        routeForThisAttempt.employees.length > 0
      ) {
        const checkIndexUnrouted = isDropoff
          ? routeForThisAttempt.employees.length - 1
          : 0;
        const criticalEmployeeUnrouted =
          routeForThisAttempt.employees[checkIndexUnrouted];
        if (
          criticalEmployeeUnrouted?.gender === "F" &&
          routeForThisAttempt.employees.some(
            (emp, idx) => idx !== checkIndexUnrouted && emp.gender === "M"
          )
        ) {
          const swapAttemptResultUnrouted = await handleGuardRequirements(
            routeForThisAttempt,
            isDropoff,
            facility,
            pickupTimePerEmployee,
            city,
            shiftTime
          );
          if (swapAttemptResultUnrouted.swapped) {
            unroutedRouteModifiedBySwap = true;
            updateRouteWithDetails(
              routeForThisAttempt,
              swapAttemptResultUnrouted.routeDetails
            );
          }
        }
      }

      // Final guard determination
      if (
        activateGuardSystemFromInput &&
        routeForThisAttempt.employees.length > 0
      ) {
        const finalCheckIndexUnrouted = isDropoff
          ? routeForThisAttempt.employees.length - 1
          : 0;
        if (
          routeForThisAttempt.employees[finalCheckIndexUnrouted]?.gender === "F"
        ) {
          routeForThisAttempt.guardNeeded = true;
        } else {
          routeForThisAttempt.guardNeeded = false;
        }
      } else {
        routeForThisAttempt.guardNeeded = false;
      }

      // STRICT deviation check - minimal tolerance only
      if (
        await checkRouteDeviationForUnrouted(
          routeForThisAttempt,
          facility,
          profile,
          true
        )
      ) {
        successfullyRoutedThisIteration = true;

        // Guard capacity check
        if (routeForThisAttempt.guardNeeded) {
          let passengerCapacity = routeForThisAttempt.vehicleCapacity - 1;
          if (routeForThisAttempt.isSpecialNeedsRoute)
            passengerCapacity = Math.min(passengerCapacity, 1);
          if (
            routeForThisAttempt.employees.length >
            Math.max(0, passengerCapacity)
          ) {
            assignErrorState(
              routeForThisAttempt,
              "Unrouted group too large for vehicle with guard"
            );
            currentEmployeesInRouteAttempt.forEach((e) => {
              processedInThisUnroutedPass.delete(e.empCode);
              remainingToRouteIteratively.unshift(e);
            });
            successfullyRoutedThisIteration = false;
            break;
          }
        }

        calculatePickupTimes(
          routeForThisAttempt,
          shiftTime,
          pickupTimePerEmployee,
          reportingTime
        );
        routeForThisAttempt.swapped = unroutedRouteModifiedBySwap;
        processedRoutes.push(routeForThisAttempt);
        console.log(
          `[Unrouted Success] Route ${
            routeForThisAttempt.uniqueKey
          } created with ${
            routeForThisAttempt.employees.length
          } employees. Distance: ${(
            (routeForThisAttempt.routeDetails?.totalDistance || 0) / 1000
          ).toFixed(2)}km`
        );
        break;
      } else {
        // Deviation failed - try trimming or fail
        if (
          trimAttempt < MAX_TRIM_ATTEMPTS_PER_GROUP &&
          currentEmployeesInRouteAttempt.length > 1
        ) {
          // Remove one employee and try again
          let empToTrim;
          if (isDropoff) {
            empToTrim = currentEmployeesInRouteAttempt.pop(); // Remove last (farthest)
          } else {
            empToTrim = currentEmployeesInRouteAttempt.shift(); // Remove first (farthest)
          }

          if (empToTrim) {
            console.log(
              `[Deviation Trim] Route ${routeForThisAttempt.uniqueKey} failed deviation check. Trimming employee ${empToTrim.empCode}. Retrying with ${currentEmployeesInRouteAttempt.length} employees.`
            );
            processedInThisUnroutedPass.delete(empToTrim.empCode);
            remainingToRouteIteratively.unshift(empToTrim);
          }
        } else {
          // Can't trim anymore or single employee failed - mark as unrouteable
          console.warn(
            `[Deviation Failed] Route ${routeForThisAttempt.uniqueKey} failed deviation check after ${trimAttempt} attempts. Marking employees as unrouteable.`
          );
          currentEmployeesInRouteAttempt.forEach((e) => {
            processedInThisUnroutedPass.delete(e.empCode);
            impossibleEmployees.push(e); // These employees can't be routed due to deviation
          });
          currentEmployeesInRouteAttempt = [];
          break;
        }
      }
    } // End trimming loop

    if (
      !successfullyRoutedThisIteration &&
      currentEmployeesInRouteAttempt?.length > 0
    ) {
      currentEmployeesInRouteAttempt.forEach((e) => {
        processedInThisUnroutedPass.delete(e.empCode);
        remainingToRouteIteratively.unshift(e);
      });
    }
  } // End main processing loop

  // Final remaining unrouted
  const finalRemainingUnrouted = [
    ...remainingToRouteIteratively.filter(
      (e) => !processedInThisUnroutedPass.has(e.empCode)
    ),
    ...impossibleEmployees,
  ].filter(
    (emp, index, self) =>
      index === self.findIndex((e) => e.empCode === emp.empCode)
  );

  if (globalIterationCount >= MAX_GLOBAL_ITERATIONS) {
    console.warn(
      `[Circuit Breaker] Stopped unrouted processing after ${MAX_GLOBAL_ITERATIONS} iterations.`
    );
  }

  console.log(
    `[Unrouted Summary] Created ${processedRoutes.length} routes (including ${singletonCandidates.length} forced singletons). ${finalRemainingUnrouted.length} employees remain unrouted.`
  );

  return {
    processedRoutes,
    remainingUnrouted: finalRemainingUnrouted,
    updatedRouteCount: currentRouteCount,
  };
}

async function generateRoutes(data) {
  try {
    const {
      employees,
      facility,
      shiftTime,
      date,
      profile,
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

    const city = profile?.name || "ncr";
    const activateGuardSystemFromInput = guard; // This is the master switch from input
    console.log(
      `Generating routes for ${employees.length} employees, date: ${date}, shift: ${shiftTime}, city: ${city}, activateGuardSystem: ${activateGuardSystemFromInput}`
    );
    if (!(await isOsrmAvailable(profile)))
      throw new Error("OSRM routing service unavailable");

    let availableFleetCounts = {};
    if (profile.fleet && Array.isArray(profile.fleet)) {
      profile.fleet.forEach((vehicle) => {
        availableFleetCounts[vehicle.type] = vehicle.count;
      });
    } else {
      console.warn(
        "[Fleet] profile.fleet is missing or invalid. Fleet features will be limited."
      );
    }
    let masterUnroutedPool = [];

    const useZones = profile.zoneBasedRouting !== false;
    let employeesByZone = {};
    const ensureSpecialFlags = (emp) => ({
      ...emp,
      isMedical: emp.isMedical || false,
      isPWD: emp.isPWD || false,
      isNMT: emp.isNMT || false,
      isOOB: emp.isOOB || false,
    });

    if (useZones) {
      let zones = data.zones || [];
      if (!zones.length && ZONES_DATA_FILE) {
        try {
          zones = await loadZonesData();
          if (!zones.length) console.warn("No zones data loaded from file.");
        } catch (err) {
          console.error(
            `Failed to load zones from ${ZONES_DATA_FILE}: ${err.message}. Proceeding without file-based zones.`
          );
        }
      }
      if (zones.length === 0 && (!data.zones || data.zones.length === 0)) {
        console.warn(
          "No zones provided in request or loaded from backend. All employees will be in DEFAULT_ZONE."
        );
      }
      employeesByZone = assignEmployeesToZones(
        employees.map(ensureSpecialFlags),
        zones
      );
    } else {
      employeesByZone = {
        GLOBAL: employees.map((emp) => ({
          ...ensureSpecialFlags(emp),
          zone: "GLOBAL",
          location: { lat: emp.geoY, lng: emp.geoX },
        })),
      };
    }
    if (Object.keys(employeesByZone).length === 0 && employees.length > 0) {
      employeesByZone["DEFAULT_ZONE"] = employees.map((emp) => ({
        ...ensureSpecialFlags(emp),
        zone: "DEFAULT_ZONE",
        location: { lat: emp.geoY, lng: emp.geoX },
      }));
    }

    const routeDataContainer = {
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

    const isDropoff = tripType.toLowerCase() === "dropoff";
    const facilityCoordinates = [facility.geoY, facility.geoX];

    const processZoneOrGroup = async (
      empsInScope,
      zoneIdentifier,
      targetHeuristicCapacity
    ) => {
      if (empsInScope.length === 0) return;
      const { routes: batchRoutes, employeesAddedToMasterUnrouted } =
        await processEmployeeBatch(
          empsInScope,
          targetHeuristicCapacity,
          facility,
          tripType,
          profileMaxDuration,
          pickupTimePerEmployee,
          activateGuardSystemFromInput, // Pass the master guard activation flag
          profile,
          availableFleetCounts,
          city,
          shiftTime
        );
      if (employeesAddedToMasterUnrouted?.length > 0) {
        employeesAddedToMasterUnrouted.forEach((emp) =>
          masterUnroutedPool.push(emp)
        );
      }
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
      if (
        !initialRoute.employees ||
        initialRoute.employees.length === 0 ||
        initialRoute.error
      ) {
        if (initialRoute.error)
          console.warn(
            `Skipping OR-Tools for errored route ${initialRoute.uniqueKey}: ${initialRoute.errorMessage}`
          );
        continue;
      }
      try {
        const {
          routes: orToolsSolvedRouteList,
          droppedEmployees,
          error: orError,
        } = await solveZoneWithORTools(
          initialRoute.employees,
          facility,
          initialRoute.vehicleCapacity,
          profileMaxDuration,
          pickupTimePerEmployee,
          tripType,
          initialRoute.zone,
          true,
          city
        );
        if (orError) throw new Error(orError.message || orError);
        if (droppedEmployees?.length > 0)
          masterUnroutedPool.push(...droppedEmployees);

        if (orToolsSolvedRouteList?.[0]?.employees.length > 0) {
          allOptimizedOrToolsRoutes.push({
            ...initialRoute,
            employees: orToolsSolvedRouteList[0].employees,
          });
        } else {
          allOptimizedOrToolsRoutes.push(initialRoute);
        }
      } catch (error) {
        console.error(
          `[OR-Tools Stage] Error optimizing route ${initialRoute.uniqueKey}: ${error.message}. Adding its employees to unrouted.`
        );
        masterUnroutedPool.push(...initialRoute.employees);
      }
    }

    const finalProcessedRoutes = [];
    const collectedUnroutedForReinsertionMap = new Map();

    // Inside generateRoutes function
    // ... (other parts of the function) ...

    for (const route of allOptimizedOrToolsRoutes) {
      totalRouteCount++;
      route.routeNumber = totalRouteCount;

      // Ensure route.employees exists and has a valid structure before proceeding
      if (!route.employees || !Array.isArray(route.employees)) {
        assignErrorState(
          route,
          "Route has invalid or missing employee list before swap attempt."
        );
        finalProcessedRoutes.push(route);
        continue;
      }

      if (
        !route.error &&
        (!route.routeDetails || Object.keys(route.routeDetails).length === 0)
      ) {
        // This can happen if the route came from OR-Tools directly without prior OSRM details
        // or if it's a fresh route shell. Calculate initial details.
        if (route.employees.length > 0) {
          const currentRouteCoords = route.employees.map((emp) => [
            emp.location.lat,
            emp.location.lng,
          ]);
          const currentAllCoords = isDropoff
            ? [facilityCoordinates, ...currentRouteCoords]
            : [...currentRouteCoords, facilityCoordinates];
          const initialDetails = await calculateRouteDetails(
            currentAllCoords,
            route.employees,
            pickupTimePerEmployee,
            tripType,
            city,
            shiftTime
          );
          if (initialDetails.error) {
            assignErrorState(
              route,
              `Initial OSRM failed: ${initialDetails.error}`
            );
            // Potentially add employees to unrouted if OSRM fails here
            route.employees.forEach((e) =>
              collectedUnroutedForReinsertionMap.set(e.empCode, e)
            );
            finalProcessedRoutes.push(route);
            continue;
          }
          updateRouteWithDetails(route, initialDetails);
        } else if (!route.error) {
          // No employees, but not an error state yet
          // This route might be empty from a previous step, ensure it's handled or errored if unexpected
          if (
            allInitiallyFormedRoutes.find(
              (r) => r.uniqueKey === route.uniqueKey && r.employees?.length > 0
            )
          ) {
            assignErrorState(
              route,
              "Route became empty before swap logic without explicit error."
            );
          }
          finalProcessedRoutes.push(route);
          continue;
        }
      }
      // If route is already in an error state, skip swap logic
      if (route.error) {
        finalProcessedRoutes.push(route);
        continue;
      }

      let currentRouteDetails = route.routeDetails; // Should be populated now
      let routeModifiedByGuardSwap = false;
      route.swappedPairInfo = null; // Initialize on the route object

      // --- Experiential Swap Attempt ---
      if (activateGuardSystemFromInput && route.employees.length > 0) {
        const checkIndex = isDropoff ? route.employees.length - 1 : 0;
        // Ensure criticalEmployee can be accessed safely
        const criticalEmployee =
          route.employees && route.employees[checkIndex]
            ? route.employees[checkIndex]
            : null;

        if (
          criticalEmployee &&
          criticalEmployee.gender === "F" &&
          route.employees.some(
            (emp, idx) => idx !== checkIndex && emp.gender === "M"
          )
        ) {
          console.log(
            `[Guard Logic - Swap Attempt] Route ${route.uniqueKey}: Critical is Female, attempting experiential swap.`
          );
          const swapAttemptResult = await handleGuardRequirements(
            route, // Pass the current route object, which includes its current .employees and .routeDetails
            isDropoff,
            facility,
            pickupTimePerEmployee,
            city,
            shiftTime
          );

          if (swapAttemptResult.swapped) {
            routeModifiedByGuardSwap = true; // Mark that a swap logic was successfully applied
            finalTotalSwappedRoutes++;
            route.swappedPairInfo = swapAttemptResult.swappedPair;

            // The swapAttemptResult.routeDetails contains the new employee order (simply swapped)
            // and the OSRM calculations for that new order.
            updateRouteWithDetails(route, swapAttemptResult.routeDetails);
            currentRouteDetails = route.routeDetails; // Update local var for consistency

            console.log(
              `[Guard Logic] Experiential swap for route ${route.uniqueKey} successful. Swapped: ${swapAttemptResult.swappedPair.originalCriticalEmpCode} with ${swapAttemptResult.swappedPair.swappedInEmpCode}.`
            );
            if (isDropoff) {
              console.log(
                `[Guard Logic - Dropoff] Route ${route.uniqueKey}: Used simply swapped order. No further OR-Tools re-optimization for this swap.`
              );
            } else {
              console.log(
                `[Guard Logic - Pickup] Route ${route.uniqueKey}: Will proceed to OR-Tools re-optimization for this swap.`
              );
            }
          } else {
            console.log(
              `[Guard Logic] Experiential swap for route ${route.uniqueKey} did not occur or was rejected.`
            );
          }
        }
      }

      // --- Final Guard Needed Determination (after potential swap) ---
      // This uses the route.employees list, which might have been updated by the swap.
      if (activateGuardSystemFromInput && route.employees.length > 0) {
        const finalCheckIndex = isDropoff ? route.employees.length - 1 : 0;
        const finalCriticalEmployee =
          route.employees && route.employees[finalCheckIndex]
            ? route.employees[finalCheckIndex]
            : null;
        if (finalCriticalEmployee && finalCriticalEmployee.gender === "F") {
          route.guardNeeded = true;
        } else {
          route.guardNeeded = false;
        }
      } else {
        route.guardNeeded = false;
      }

      // --- Re-Optimization Call (Conditional) ---
      // Only perform OR-Tools re-optimization if it was a PICKUP trip AND a swap occurred.
      // For DROP-OFF, if a swap occurred, `handleGuardRequirements` already provided the
      // simply swapped order and its OSRM details. We don't re-optimize it further.
      // --- Re-Optimization Call (Conditional) ---
      // Perform OR-Tools re-optimization if a swap occurred, for ANY trip type.
      if (routeModifiedByGuardSwap) {
        console.log(
          `[Guard Logic - Re-Opt] Route ${route.uniqueKey}: ${tripType} trip, re-optimizing after swap.`
        );
        const reOptResult = await reOptimizeSwappedRouteWithORTools(
          route,
          facility,
          pickupTimePerEmployee,
          city
        );
        if (reOptResult.reOptimized && reOptResult.employees.length > 0) {
          route.employees = reOptResult.employees; // Update with re-optimized employee order
          // Recalculate OSRM details for this new re-optimized order
          const reOptRouteCoords = route.employees.map((emp) => [
            emp.location.lat,
            emp.location.lng,
          ]);
          const reOptAllCoords = isDropoff
            ? [facilityCoordinates, ...reOptRouteCoords]
            : [...reOptRouteCoords, facilityCoordinates];
          const reOptDetails = await calculateRouteDetails(
            reOptAllCoords,
            route.employees,
            pickupTimePerEmployee,
            tripType,
            city,
            shiftTime
          );

          if (reOptDetails.error) {
            console.warn(
              `[Guard Logic - Re-Opt] OSRM failed after re-optimization for ${route.uniqueKey}: ${reOptDetails.error}. Using pre-reopt swapped order (from handleGuardRequirements).`
            );
          } else {
            updateRouteWithDetails(route, reOptDetails); // Update with successfully re-optimized details
          }
        } else if (reOptResult.error) {
          console.warn(
            `[Guard Logic - Re-Opt] Re-optimization failed for ${route.uniqueKey}: ${reOptResult.error}. Using pre-reopt swapped order.`
          );
        } else {
          console.warn(
            `[Guard Logic - Re-Opt] Re-optimization did not return a valid route for ${route.uniqueKey}. Using pre-reopt swapped order.`
          );
        }
      }

      // --- Post-processing after potential swap and re-optimization ---
      // Guard capacity check (if guardNeeded is true after all modifications)
      if (route.guardNeeded) {
        // ... (your existing guard capacity trimming logic) ...
        // Remember to recalculate OSRM details if employees are trimmed here.
      }

      if (route.employees.length === 0) {
        if (!route.error)
          assignErrorState(route, "Route became empty post-processing/swap");
        finalProcessedRoutes.push(route);
        continue;
      }

      // Final deviation check
      if (!(await checkRouteDeviation(route, facility, profile))) {
        assignErrorState(
          route,
          "Exceeded acceptable route deviation after swap/re-opt"
        );
        route.employees.forEach((e) =>
          collectedUnroutedForReinsertionMap.set(e.empCode, e)
        );
        finalProcessedRoutes.push(route);
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
        route.routeDetails?.totalDuration > profileMaxDuration
      ) {
        route.durationExceeded = true;
      }
      route.swapped = routeModifiedByGuardSwap; // Set the final swapped status
      finalProcessedRoutes.push(route);
    } // End main processing loop for allOptimizedOrToolsRoutes

    // ... (rest of generateRoutes) // End main processing loop for allOptimizedOrToolsRoutes

    routeDataContainer.routeData = [...finalProcessedRoutes];
    masterUnroutedPool.forEach((e) =>
      collectedUnroutedForReinsertionMap.set(e.empCode, e)
    );

    const successfullyRoutedEmpCodes = new Set();
    routeDataContainer.routeData.forEach((r) => {
      if (!r.error && r.employees)
        r.employees.forEach((e) => successfullyRoutedEmpCodes.add(e.empCode));
    });
    const finalUnroutedForProcessing = Array.from(
      collectedUnroutedForReinsertionMap.values()
    ).filter(
      (emp) =>
        emp && emp.empCode && !successfullyRoutedEmpCodes.has(emp.empCode)
    );

    // --- UNROUTED HANDLING WITH ITERATIVE TRIMMING (STRATEGY 2) ---
    // Replace the existing unrouted handling section with:
    if (finalUnroutedForProcessing.length > 0) {
      const unroutedResult = await processUnroutedEmployeesWithSafeguards(
        finalUnroutedForProcessing,
        profile,
        availableFleetCounts,
        facility,
        tripType,
        profileMaxDuration,
        pickupTimePerEmployee,
        activateGuardSystemFromInput,
        shiftTime,
        city,
        totalRouteCount,
        routeDataContainer,
        reportingTime
      );

      // Add successful routes to the container
      routeDataContainer.routeData.push(...unroutedResult.processedRoutes);
      totalRouteCount = unroutedResult.updatedRouteCount;

      // Update master unrouted pool with remaining
      masterUnroutedPool = unroutedResult.remainingUnrouted;
    }

    // --- END OF UNROUTED HANDLING ---

    const finalStats = calculateRouteStatistics(
      routeDataContainer,
      employees.length
    );
    const response = await createSimplifiedResponse({
      ...routeDataContainer,
      ...finalStats,
      totalSwappedRoutes: finalTotalSwappedRoutes,
    });

    const allEffectivelyRoutedEmpCodes = new Set();
    routeDataContainer.routeData.forEach((r) => {
      if (!r.error && r.employees)
        r.employees.forEach((e) => allEffectivelyRoutedEmpCodes.add(e.empCode));
    });
    response.unroutedEmployees = employees
      .filter((emp) => !allEffectivelyRoutedEmpCodes.has(emp.empCode))
      .concat(
        masterUnroutedPool.filter(
          (emp) => !allEffectivelyRoutedEmpCodes.has(emp.empCode)
        )
      )
      .filter(
        (emp, index, self) =>
          emp &&
          emp.empCode &&
          index === self.findIndex((e) => e.empCode === emp.empCode)
      )
      .map((emp) => ({
        empCode: emp.empCode,
        geoX: emp.geoX,
        geoY: emp.geoY,
        gender: emp.gender,
        isMedical: emp.isMedical || false,
        isPWD: emp.isPWD || false,
      }));

    return response;
  } catch (error) {
    console.error("Top-level generateRoutes error:", error.stack);
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
      throw new Error("Invalid input parameters for calculatePickupTimes");
    }
    
    // Get dynamic traffic buffer - but cap it at reasonable levels
    let TRAFFIC_BUFFER_PERCENTAGE = getTrafficBufferForShiftTime(shiftTime);
    
    // Cap the traffic buffer to prevent unreasonable delays
    TRAFFIC_BUFFER_PERCENTAGE = Math.min(TRAFFIC_BUFFER_PERCENTAGE, 0.4); // Max 40%
    
    console.log(`[DEBUG] Traffic buffer for shift ${shiftTime}: ${(TRAFFIC_BUFFER_PERCENTAGE * 100).toFixed(1)}%`);

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

    // Create target time
    const facilityTargetTime = new Date();
    facilityTargetTime.setHours(hours, minutes, 0, 0);
    const isDropoff = route.tripType?.toLowerCase() === "dropoff";

    console.log(`[DEBUG] Facility target time: ${formatTime(facilityTargetTime)}`);
    console.log(`[DEBUG] Reporting time: ${reportingTimeSeconds} seconds`);

    if (!isDropoff) {
      // PICKUP LOGIC
      // Calculate when employees should arrive at facility (before shift time)
      let targetFacilityArrivalTime = new Date(facilityTargetTime);
      if (reportingTimeSeconds > 0) {
        targetFacilityArrivalTime = new Date(facilityTargetTime.getTime() - (reportingTimeSeconds * 1000));
      }
      
      route.facilityArrivalTime = formatTime(targetFacilityArrivalTime);
      console.log(`[DEBUG] Target facility arrival: ${route.facilityArrivalTime}`);

      // Start from facility arrival time and work backwards
      let currentTimeMs = targetFacilityArrivalTime.getTime();
      
      // Debug: Print all leg durations
      console.log(`[DEBUG] Route legs:`, route.routeDetails?.legs?.map((leg, idx) => ({
        index: idx,
        duration: leg.duration,
        durationMin: (leg.duration / 60).toFixed(1)
      })));
      
      // Work backwards through employees
      for (let i = route.employees.length - 1; i >= 0; i--) {
        const employee = route.employees[i];
        
        // Get the leg duration for travel FROM this employee TO the next destination
        let legDuration = 0;
        
        if (i === route.employees.length - 1) {
          // Last employee: travel time to facility (should be last leg)
          const lastLegIndex = route.routeDetails?.legs?.length - 1;
          if (lastLegIndex >= 0 && route.routeDetails.legs[lastLegIndex]) {
            legDuration = route.routeDetails.legs[lastLegIndex].duration || 0;
          }
          console.log(`[DEBUG] Employee ${employee.empCode} (last): using leg ${lastLegIndex}, duration: ${(legDuration/60).toFixed(1)}min`);
        } else {
          // Other employees: travel time to next employee
          if (route.routeDetails?.legs?.[i]) {
            legDuration = route.routeDetails.legs[i].duration || 0;
          }
          console.log(`[DEBUG] Employee ${employee.empCode}: using leg ${i}, duration: ${(legDuration/60).toFixed(1)}min`);
        }
        
        // Apply traffic buffer
        const bufferedLegDuration = legDuration * (1 + TRAFFIC_BUFFER_PERCENTAGE);
        
        // Subtract travel time to next destination
        currentTimeMs -= (bufferedLegDuration * 1000);
        
        // Subtract pickup time for this employee
        currentTimeMs -= (pickupTimePerEmployee * 1000);
        
        // Set pickup time
        const pickupTime = new Date(currentTimeMs);
        employee.pickupTime = formatTime(pickupTime);
        
        console.log(`[DEBUG] Employee ${employee.empCode}: pickup at ${employee.pickupTime} (leg: ${(legDuration/60).toFixed(1)}min, buffered: ${(bufferedLegDuration/60).toFixed(1)}min, pickup service: ${pickupTimePerEmployee}s)`);
      }
      
    } else {
      // DROPOFF LOGIC (existing logic should be fine)
      let currentTimeMs = facilityTargetTime.getTime();
      route.facilityDepartureTime = formatTime(facilityTargetTime);
      
      for (let i = 0; i < route.employees.length; i++) {
        const employee = route.employees[i];
        const legToThisEmployee = route.routeDetails?.legs?.[i];
        const legDuration = (legToThisEmployee?.duration || 0) * (1 + TRAFFIC_BUFFER_PERCENTAGE);
        
        currentTimeMs += (legDuration * 1000);
        currentTimeMs += (pickupTimePerEmployee * 1000);
        
        const dropoffTime = new Date(currentTimeMs);
        employee.dropoffTime = formatTime(dropoffTime);
        employee.pickupTime = employee.dropoffTime;
      }
    }
    
  } catch (error) {
    console.error("Time calculation error:", error.message);
    if (route?.employees) {
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
  route, // The route object
  isDropoff,
  facility,
  pickupTimePerEmployee,
  city,
  shiftTime
) {
  const fastApiCity = getFastApiCityKey(city);
  let detailsToUse = route.routeDetails; // Start with original details
  let swappedOccurred = false;
  let finalSwappedPairInfo = null; // Initialize to null

  try {
    // Initial checks: if any of these fail, no swap, return early.
    if (!route.employees || route.employees.length < 2) {
      return {
        swapped: false,
        routeDetails: route.routeDetails,
        swappedPair: null,
      };
    }

    const checkIndex = isDropoff ? route.employees.length - 1 : 0;
    const criticalEmployee = route.employees[checkIndex]; // Defined here

    if (criticalEmployee?.gender !== "F") {
      return {
        swapped: false,
        routeDetails: route.routeDetails,
        swappedPair: null,
      };
    }

    const swappableMaleCandidates = route.employees.filter(
      (emp, index) => index !== checkIndex && emp.gender === "M"
    );

    if (swappableMaleCandidates.length === 0) {
      console.log(
        `[Guard Swap] Route ${route.uniqueKey}: Critical is Female but no males available to swap. Guard will be assigned.`
      );
      return {
        swapped: false,
        routeDetails: route.routeDetails,
        swappedPair: null,
      };
    }

    // --- OSRM and Candidate Logic ---
    // Prepare coordinates as [lng, lat] pairs for FastAPI
    const osrmCoordinates = [
      [criticalEmployee.location.lng, criticalEmployee.location.lat],
      ...swappableMaleCandidates.map((emp) => [
        emp.location.lng,
        emp.location.lat,
      ]),
    ];
    const sources = [0];
    const destinations = swappableMaleCandidates.map((_, i) => i + 1);
    // POST to FastAPI /table endpoint
    const response = await fetchApi(`${FASTAPI_GATEWAY_URL}/table`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        city: fastApiCity,
        coordinates: osrmCoordinates,
        sources,
        destinations,
        annotations: "distance",
      }),
      timeout: OSRM_PROBE_TIMEOUT_HEURISTIC,
    });
    // Debug log for FastAPI response
    const data = await response.json();
    console.log(
      "[DEBUG handleGuardRequirements] FastAPI /table response:",
      JSON.stringify(data)
    );
    let osrmDistances = [];
    if (response.ok) {
      if (data.code === "Ok" && data.distances?.[0]) {
        osrmDistances = data.distances[0];
      } else {
        console.warn(
          `[GuardSwap - Experiential] FastAPI /table error for ${route.uniqueKey}: ${data.message}`
        );
        return {
          swapped: false,
          routeDetails: route.routeDetails,
          swappedPair: null,
        };
      }
    } else {
      console.warn(
        `[GuardSwap - Experiential] FastAPI /table HTTP error for ${route.uniqueKey}: ${response.status}`
      );
      return {
        swapped: false,
        routeDetails: route.routeDetails,
        swappedPair: null,
      };
    }

    const validCandidatesForSwap = [];
    swappableMaleCandidates.forEach((maleEmp, idx) => {
      const roadDistanceMeters = osrmDistances[idx + 1]; // OSRM distances are 1-indexed from the destinations array
      // / --- START DEBUG LOGGING (Keep this for verification) ---
      console.log(`[DEBUG SWAP CHECK] Route: ${route.uniqueKey}`);
      console.log(
        `  Critical Female: ${criticalEmployee.empCode} at [${criticalEmployee.location.lat}, ${criticalEmployee.location.lng}]`
      );
      console.log(
        `  Male Candidate: ${maleEmp.empCode} at [${maleEmp.location.lat}, ${maleEmp.location.lng}] (Index in swappableMaleCandidates: ${idx})`
      );
      // --- END DEBUG LOGGING ---

      if (roadDistanceMeters != null) {
        const roadDistanceKm = roadDistanceMeters / 1000;

        // --- MORE DEBUG LOGGING (Keep this for verification) ---
        console.log(
          `  OSRM Road Distance (Meters): ${roadDistanceMeters}, (KM): ${roadDistanceKm.toFixed(
            3
          )}`
        );
        console.log(`  MAX_SWAP_DISTANCE_KM: ${MAX_SWAP_DISTANCE_KM}`);
        console.log(
          `  Condition Check: ${roadDistanceKm.toFixed(
            3
          )} <= ${MAX_SWAP_DISTANCE_KM} is ${
            roadDistanceKm <= MAX_SWAP_DISTANCE_KM
          }`
        );
        // --- END DEBUG LOGGING ---

        if (roadDistanceKm <= MAX_SWAP_DISTANCE_KM) {
          console.log(`    -> Candidate ${maleEmp.empCode} IS VALID for swap.`); // Debug
          validCandidatesForSwap.push({
            employee: maleEmp,
            indexInRoute: route.employees.findIndex(
              (e) => e.empCode === maleEmp.empCode
            ),
            distance: roadDistanceKm,
          });
        } else {
          console.log(
            `    -> Candidate ${maleEmp.empCode} IS INVALID for swap (too far).`
          ); // Debug
        }
      } else {
        console.log(`  OSRM Road Distance: NULL for this pair.`); // Debug
        console.log(
          `    -> Candidate ${maleEmp.empCode} IS INVALID for swap (no distance).`
        ); // Debug
      }
      console.log(`--- END DEBUG FOR CANDIDATE ${maleEmp.empCode} ---`); // Debug
    });

    if (validCandidatesForSwap.length === 0) {
      console.log(
        `[Guard Swap] Route ${route.uniqueKey}: No male candidates within ${MAX_SWAP_DISTANCE_KM}km range. Guard will be assigned.`
      );
      return {
        swapped: false,
        routeDetails: route.routeDetails,
        swappedPair: null,
      };
    }
    // --- End of OSRM and Candidate Logic ---

    validCandidatesForSwap.sort((a, b) => a.distance - b.distance);
    const bestMaleToSwap = validCandidatesForSwap[0]; // Now bestMaleToSwap is guaranteed to be defined if we reach here

    // Perform the swap
    const newEmployees = [...route.employees];
    [newEmployees[checkIndex], newEmployees[bestMaleToSwap.indexInRoute]] = [
      newEmployees[bestMaleToSwap.indexInRoute],
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
      route.tripType,
      city,
      shiftTime
    );

    if (routeDetailsAfterSwap.error) {
      console.warn(
        `[GuardSwap - Experiential] OSRM failed for swapped route ${route.uniqueKey}: ${routeDetailsAfterSwap.error}. Guard will be assigned.`
      );
      return {
        swapped: false,
        routeDetails: route.routeDetails,
        swappedPair: null,
      };
    }

    const originalDuration = route.routeDetails?.totalDuration || Infinity;
    const newDuration = routeDetailsAfterSwap.totalDuration;
    const durationIncreasePercentage =
      newDuration > originalDuration
        ? (newDuration - originalDuration) / originalDuration
        : 0;

    if (durationIncreasePercentage > 0.25) {
      console.warn(
        `[GuardSwap - Experiential] Route ${
          route.uniqueKey
        } swap increases duration by ${(
          durationIncreasePercentage * 100
        ).toFixed(1)}%. Rejecting swap. Guard will be assigned.`
      );
      return {
        swapped: false,
        routeDetails: route.routeDetails,
        swappedPair: null,
      };
    }

    // If all checks pass, the swap is successful
    console.log(
      `[GuardSwap - Experiential] Successfully swapped ${criticalEmployee.empCode} (F) with ${bestMaleToSwap.employee.empCode} (M) in route ${route.uniqueKey}. Guard saved!`
    );
    swappedOccurred = true;
    detailsToUse = routeDetailsAfterSwap;
    finalSwappedPairInfo = {
      // Construct the info here, where all variables are valid
      originalCriticalEmpCode: criticalEmployee.empCode,
      swappedInEmpCode: bestMaleToSwap.employee.empCode,
    };
  } catch (error) {
    console.error(
      `Error in handleGuardRequirements (experiential swap) for route ${route?.uniqueKey}:`,
      error
    );
    // In case of any unexpected error, ensure swappedOccurred is false and swappedPair is null
    swappedOccurred = false; // Explicitly set to false
    finalSwappedPairInfo = null; // Explicitly set to null
    // detailsToUse might be the original if the error happened early.
  }

  return {
    swapped: swappedOccurred,
    routeDetails: detailsToUse,
    swappedPair: finalSwappedPairInfo,
  };
}

function assignErrorState(route, message = "Unknown error") {
  if (!route) return;
  console.warn(
    `Assigning error state to route ${
      route.uniqueKey || route.routeNumber || "UNKNOWN"
    }: ${message}`
  );
  route.employees = (route.employees || []).map((e, i) => ({
    ...e,
    order: i + 1,
    pickupTime: "Error",
    dropoffTime: "Error",
  }));
  route.encodedPolyline = "error_polyline";
  route.routeDetails = { totalDistance: 0, totalDuration: 0, legs: [] };
  route.swapped = false;
  route.error = true;
  route.errorMessage = message;
}

function updateRouteWithDetails(route, routeDetails) {
  if (!route || !routeDetails) return;
  if (routeDetails.error) {
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

function calculateRouteStatistics(routeDataContainer, totalEmployeesInput) {
  const validRoutes = routeDataContainer.routeData.filter(
    (route) => !route.error && route.employees?.length > 0
  );
  const totalValidRoutes = validRoutes.length;
  const totalRoutedEmployees = validRoutes.reduce(
    (sum, route) => sum + route.employees.length,
    0
  );
  const averageOccupancy =
    totalValidRoutes > 0 ? totalRoutedEmployees / totalValidRoutes : 0;

  // Calculate total guarded routes
  const totalGuardedRoutes = validRoutes.filter(
    (route) => route.guardNeeded === true
  ).length;

  let totalDistanceSum = 0;
  let totalDurationSum = 0;
  validRoutes.forEach((route) => {
    totalDistanceSum += route.routeDetails?.totalDistance || 0;
    totalDurationSum += route.routeDetails?.totalDuration || 0;
  });

  return {
    totalEmployees: totalEmployeesInput,
    totalRoutedEmployees,
    totalRoutes: totalValidRoutes,
    totalGuardedRoutes, // NEW: Total routes with guards
    averageOccupancy: parseFloat(averageOccupancy.toFixed(2)),
    overallRouteDetails: {
      totalDistance: parseFloat((totalDistanceSum / 1000).toFixed(2)),
      totalDuration: parseFloat(totalDurationSum.toFixed(2)),
    },
  };
}

async function createSimplifiedResponse(routeDataContainer) {
  const city = routeDataContainer.profile?.name || "ncr";
  const fastApiCity = getFastApiCityKey(city);
  const isDropoffTrip = routeDataContainer.tripType === "DROPOFF";

  // Process routes with OSRM distance calculations
  const processedRoutes = await Promise.all(
    routeDataContainer.routeData
      .filter((route) => !route.error && route.employees?.length > 0)
      .map(async (route) => {
        const guardAssigned = route.guardNeeded || false;
        const occupancy = route.employees?.length || 0;

        // Calculate farthest employee distance using OSRM
        let farthestEmployeeDistance = 0;

        if (
          route.employees &&
          route.employees.length > 0 &&
          routeDataContainer.facility?.geoY &&
          routeDataContainer.facility?.geoX
        ) {
          // Identify the farthest employee
          const farthestEmployee = isDropoffTrip
            ? route.employees[route.employees.length - 1] // Last dropoff
            : route.employees[0]; // First pickup

          if (
            farthestEmployee?.location?.lat &&
            farthestEmployee?.location?.lng
          ) {
            try {
              const coords = isDropoffTrip
                ? [
                    [
                      routeDataContainer.facility.geoX,
                      routeDataContainer.facility.geoY,
                    ],
                    [
                      farthestEmployee.location.lng,
                      farthestEmployee.location.lat,
                    ],
                  ]
                : [
                    [
                      farthestEmployee.location.lng,
                      farthestEmployee.location.lat,
                    ],
                    [
                      routeDataContainer.facility.geoX,
                      routeDataContainer.facility.geoY,
                    ],
                  ];
              const response = await fetchApi(`${FASTAPI_GATEWAY_URL}/route`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  city: fastApiCity,
                  coordinates: coords,
                  overview: "false",
                  steps: false,
                  geometries: "polyline",
                }),
                timeout: OSRM_PROBE_TIMEOUT,
              });
              if (response.ok) {
                const data = await response.json();
                if (data.code === "Ok" && data.routes?.[0]?.distance != null) {
                  farthestEmployeeDistance = data.routes[0].distance / 1000; // Convert to km
                }
              } else {
                console.warn(
                  `[FarthestDistance] OSRM failed for route ${route.uniqueKey}: ${response.status}`
                );
              }
            } catch (error) {
              console.warn(
                `[FarthestDistance] Error calculating distance for route ${route.uniqueKey}:`,
                error.message
              );
            }
          }
        }

        // Check for special employee types in the route
        const hasAnyEmployeeType = (route.employees || []).reduce(
          (flags, emp) => {
            return {
              isMedical: flags.isMedical || emp.isMedical === true,
              isPWD: flags.isPWD || emp.isPWD === true,
              isNMT: flags.isNMT || emp.isNMT === true,
              isOOB: flags.isOOB || emp.isOOB === true,
            };
          },
          { isMedical: false, isPWD: false, isNMT: false, isOOB: false }
        );

        return {
          routeNumber: route.routeNumber,
          zone: route.zone,
          vehicleCapacity: route.vehicleCapacity,
          vehicleType: route.assignedVehicleType || "UNKNOWN",
          guard: guardAssigned,
          swapped: route.swapped || false,
          swappedPairInfo: route.swappedPairInfo || null, // <<< ADD THIS LINE
          durationExceeded: route.durationExceeded || false,
          uniqueKey: route.uniqueKey,
          isSpecialNeedsRoute: route.isSpecialNeedsRoute || false,
          afterFleetExhaustion: route.afterFleetExhaustion || false,
          distance: parseFloat(
            ((route.routeDetails?.totalDistance || 0) / 1000).toFixed(2)
          ),
          duration: parseFloat(
            (route.routeDetails?.totalDuration || 0).toFixed(2)
          ),
          occupancy,
          farthestEmployeeDistance: parseFloat(
            farthestEmployeeDistance.toFixed(2)
          ),
          isMedicalRoute: hasAnyEmployeeType.isMedical,
          isPWDRoute: hasAnyEmployeeType.isPWD,
          isNMTRoute: hasAnyEmployeeType.isNMT,
          isOOBRoute: hasAnyEmployeeType.isOOB,
          encodedPolyline: route.encodedPolyline || "no_polyline",
          employees: (route.employees || []).map((emp, index) => ({
            empCode: emp.empCode,
            gender: emp.gender,
            isMedical: emp.isMedical || false,
            isPWD: emp.isPWD || false,
            isNMT: emp.isNMT || false,
            isOOB: emp.isOOB || false,
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
      })
  );

  return {
    uuid: routeDataContainer.uuid,
    date: routeDataContainer.date,
    shift: routeDataContainer.shift,
    tripType: routeDataContainer.tripType === "PICKUP" ? "P" : "D",
    totalEmployees: routeDataContainer.totalEmployees,
    totalRoutedEmployees: routeDataContainer.totalRoutedEmployees,
    totalRoutes: routeDataContainer.totalRoutes,
    totalGuardedRoutes: routeDataContainer.totalGuardedRoutes,
    averageOccupancy: routeDataContainer.averageOccupancy,
    overallRouteDetails: routeDataContainer.overallRouteDetails,
    totalSwappedRoutes: routeDataContainer.totalSwappedRoutes,
    routes: processedRoutes,
    unroutedEmployees: routeDataContainer.unroutedEmployees || [],
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
    totalGuardedRoutes: 0, // NEW: No guarded routes in empty response
    averageOccupancy: 0,
    overallRouteDetails: { totalDistance: 0, totalDuration: 0 },
    totalSwappedRoutes: 0,
    routes: [],
    unroutedEmployees: (data.employees || []).map((emp) => ({
      empCode: emp.empCode,
      geoX: emp.geoX,
      geoY: emp.geoY,
      gender: emp.gender,
      isMedical: emp.isMedical || false,
      isPWD: emp.isPWD || false,
      isNMT: emp.isNMT || false, // NEW: Include NMT flag
      isOOB: emp.isOOB || false, // NEW: Include OOB flag
    })),
  };
}

module.exports = {
  generateRoutes,
  isOsrmAvailable,
};
