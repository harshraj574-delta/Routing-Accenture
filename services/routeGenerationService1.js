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

// Set the maximum number of coordinates per OSRM table request
const OSRM_LIMIT = 100; // Adjust as needed for your OSRM build (e.g., 100 or 350)

const fetchApi = async (url, options = {}, retries = 3, delay = 1000) => {
  const { default: fetch } = await import("node-fetch");

  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, options);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return response;
    } catch (error) {
      const isRetryable =
        error.code === "ECONNRESET" ||
        error.message === "socket hang up" ||
        error.type === "system";

      if (i < retries - 1 && isRetryable) {
        console.warn(
          `Fetch failed (attempt ${i + 1} of ${retries}): ${error.message}. Retrying in ${delay}ms...`
        );
        await new Promise((res) => setTimeout(res, delay));
      } else {
        console.error(`Fetch failed: ${error.message}`);
        throw error;
      }
    }
  }
};


// const ZONES_DATA_FILE = path.join(__dirname, "../data/delhi_ncr_zones.json");
const ZONES_DATA_FILE = path.join(__dirname, "../data/bengaluru_zones.json");

// --- All your existing helper functions (isOsrmAvailable, decode/encodePolyline, etc.) ---
// Assuming fetchApi is your fetch wrapper

async function isOsrmAvailable(city) {
  let osrmBaseUrl;
  // --- Define the OSRM base URL here ---
  if(city === "ncr"){
     osrmBaseUrl = "http://3.108.58.254:5000"; // Your specified OSRM server
  }
  else if(city === "chennai"){
    osrmBaseUrl  = "http://13.235.89.143:5000";
  }
 
  // ---

  try {
    // A minimal OSRM /route call with two coordinates
    // Coordinates are lng,lat;lng,lat
    const testCoordinates = "77.1025,28.7041;77.1026,28.7042";
    // Add minimal query parameters, as we only care about a successful response
    const queryParams = "overview=false&steps=false&alternatives=false";
    const osrmCheckUrl = `${osrmBaseUrl}/route/v1/driving/${testCoordinates}?${queryParams}`;

    // console.log(`[isOsrmAvailable] Checking OSRM at: ${osrmCheckUrl}`);

    const response = await fetchApi(osrmCheckUrl, {
      method: "GET",
      timeout: 8000, // Keep a reasonable timeout
    });

    if (response.ok) {
      const data = await response.json();
      // Standard OSRM success check: 'code' should be 'Ok' and 'routes' array should exist
      return data && data.code === "Ok" && data.routes !== undefined;
    }
    console.warn(
      `[isOsrmAvailable] OSRM check failed. Status: ${response.status}`
    );
    return false;
  } catch (error) {
    console.error("Error checking OSRM availability:", error.message);
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

// Assuming OSRM_PROBE_TIMEOUT is defined
// Assuming fetchApi is your fetch wrapper

// Assuming OSRM_PROBE_TIMEOUT is defined
// Assuming fetchApi is your fetch wrapper

// Assuming OSRM_PROBE_TIMEOUT is defined
// Assuming fetchApi is your fetch wrapper

async function checkRouteDeviation(route, facility, profile) {
  let city = profile?.name || "Unknown City";
  let osrmBaseUrl;
  
  // Define the OSRM base URL
  if (city === "ncr") {
    osrmBaseUrl = "http://3.108.58.254:5000";
  } else if (city === "chennai") {
    osrmBaseUrl = "http://13.235.89.143:5000";
  }

  // Early validation checks
  if (!profile?.routeDeviationRules) {
    console.warn(
      `[checkRouteDeviation] Route ${route.routeNumber}: profile.routeDeviationRules is missing. Returning true (lenient).`
    );
    return true;
  }

  if (
    !route?.routeDetails ||
    !route?.employees ||
    route.employees.length === 0
  ) {
    console.warn(
      `[checkRouteDeviation] Route ${route.routeNumber}: Missing routeDetails or no employees. RouteDetails: ${!!route.routeDetails}, Employees: ${route.employees?.length}. Returning true (lenient).`
    );
    return true;
  }

  const facilityType = profile.facilityType || "CDC";
  console.log(`[checkRouteDeviation] Using facilityType: ${facilityType}`); // Debug log
  
  const rules =
    profile.routeDeviationRules[facilityType] ||
    profile.routeDeviationRules["DEFAULT"];

  if (!rules || rules.length === 0) {
    console.warn(
      `[checkRouteDeviation] Route ${route.routeNumber}: No deviation rules found for facilityType: ${facilityType}. Available facility types: ${Object.keys(profile.routeDeviationRules || {}).join(', ')}. Returning true (lenient).`
    );
    return true;
  }

  // Log available rules for debugging
  console.log(
    `[checkRouteDeviation] Found ${rules.length} rules for ${facilityType}:`,
    rules.map(r => `${r.minDistKm}-${r.maxDistKm}km (max: ${r.maxTotalOneWayKm}km)`).join(', ')
  );

  let maxOsrmDistToEmployeeKm = 0;
  if (route.employees.length > 0) {
    const facilityGeoX = facility.geoX;
    const facilityGeoY = facility.geoY;

    for (const emp of route.employees) {
      if (
        emp.location &&
        typeof emp.location.lng === "number" &&
        typeof emp.location.lat === "number"
      ) {
        try {
          const coordsString = `${facilityGeoX},${facilityGeoY};${emp.location.lng},${emp.location.lat}`;
          const queryParams =
            "alternatives=false&steps=false&annotations=distance";
          const url = `${osrmBaseUrl}/route/v1/driving/${coordsString}?${queryParams}`;

          const response = await fetchApi(url, {
            method: "GET",
            timeout: OSRM_PROBE_TIMEOUT,
          });

          if (response.ok) {
            const data = await response.json();
            if (
              data.code === "Ok" &&
              data.routes &&
              data.routes.length > 0 &&
              data.routes[0].distance != null
            ) {
              const distMeters = data.routes[0].distance;
              const distKm = distMeters / 1000;
              if (distKm > maxOsrmDistToEmployeeKm) {
                maxOsrmDistToEmployeeKm = distKm;
              }
            } else {
              console.warn(
                `[checkRouteDeviation OSRM] Route ${route.routeNumber}, Emp ${emp.empCode}: API error or no distance. Code: ${data.code}, Msg: ${data.message}. URL: ${url}`
              );
            }
          } else {
            const errorText = await response.text();
            console.warn(
              `[checkRouteDeviation OSRM] Route ${route.routeNumber}, Emp ${emp.empCode}: HTTP error ${response.status}. URL: ${url}. Body: ${errorText}`
            );
          }
        } catch (err) {
          console.error(
            `[checkRouteDeviation OSRM] Route ${route.routeNumber}, Emp ${emp.empCode}: Error fetching OSRM dist: ${err.message}`
          );
        }
      } else {
        console.warn(
          `[checkRouteDeviation] Route ${route.routeNumber}, Emp ${emp.empCode}: Missing location data.`
        );
      }
    }
  }

  console.log(
    `[checkRouteDeviation] Route ${route.routeNumber}: Max OSRM distance from Facility to an employee: ${maxOsrmDistToEmployeeKm.toFixed(3)} km (exact: ${maxOsrmDistToEmployeeKm})`
  );

  // Improved rule finding with floating point tolerance
  const EPSILON = 0.001; // Small tolerance for floating point comparison
  
  let applicableRule = rules.find((rule) => {
    const inRange = 
      maxOsrmDistToEmployeeKm >= (rule.minDistKm - EPSILON) &&
      maxOsrmDistToEmployeeKm <= (rule.maxDistKm + EPSILON);
    
    console.log(
      `[checkRouteDeviation] Checking rule ${rule.minDistKm}-${rule.maxDistKm}km against ${maxOsrmDistToEmployeeKm.toFixed(3)}km: ${inRange ? 'MATCH' : 'no match'}`
    );
    
    return inRange;
  });

  if (!applicableRule) {
    const sortedRules = [...rules].sort((a, b) => a.maxDistKm - b.maxDistKm);
    const lastRule = sortedRules[sortedRules.length - 1];

    console.log(
      `[checkRouteDeviation] No exact rule match for ${maxOsrmDistToEmployeeKm.toFixed(3)}km. Last rule max: ${lastRule?.maxDistKm}km`
    );

    if (lastRule && maxOsrmDistToEmployeeKm > lastRule.maxDistKm) {
      console.log(
        `[checkRouteDeviation] Distance ${maxOsrmDistToEmployeeKm.toFixed(3)}km exceeds last rule max (${lastRule.maxDistKm}km). Applying last rule.`
      );
      applicableRule = lastRule;
    } else {
      // Find the closest rule as fallback
      let closestRule = null;
      let minDistance = Infinity;

      for (const rule of rules) {
        let distance;
        if (maxOsrmDistToEmployeeKm < rule.minDistKm) {
          distance = rule.minDistKm - maxOsrmDistToEmployeeKm;
        } else if (maxOsrmDistToEmployeeKm > rule.maxDistKm) {
          distance = maxOsrmDistToEmployeeKm - rule.maxDistKm;
        } else {
          distance = 0; // Should have been caught above, but just in case
        }

        if (distance < minDistance) {
          minDistance = distance;
          closestRule = rule;
        }
      }

      if (closestRule) {
        console.log(
          `[checkRouteDeviation] Using closest rule as fallback: ${closestRule.minDistKm}-${closestRule.maxDistKm}km (distance: ${minDistance.toFixed(3)}km)`
        );
        applicableRule = closestRule;
      }
    }
  }

  if (!applicableRule || applicableRule.maxTotalOneWayKm == null) {
    console.error(
      `[checkRouteDeviation] Could not determine applicable rule. Distance: ${maxOsrmDistToEmployeeKm.toFixed(3)}km. Available rules: ${rules.map(r => `${r.minDistKm}-${r.maxDistKm}`).join(', ')}. Returning FALSE.`
    );
    return false;
  }

  console.log(
    `[checkRouteDeviation] Applicable rule: ${applicableRule.minDistKm}-${applicableRule.maxDistKm}km, maxTotalOneWayKm: ${applicableRule.maxTotalOneWayKm}`
  );

  // Calculate relevant route distance
  let relevantRouteDistanceKm;
  if (route.tripType?.toUpperCase() === "PICKUP") {
    relevantRouteDistanceKm = route.routeDetails.totalDistance / 1000;
  } else {
    relevantRouteDistanceKm = route.routeDetails.totalDistance / 1000;
  }

  const maxAllowedKmForRule = applicableRule.maxTotalOneWayKm;

  console.log(
    `[checkRouteDeviation] Actual OSRM for ENTIRE ROUTE: ${relevantRouteDistanceKm.toFixed(3)} km. Max allowed by rule: ${maxAllowedKmForRule} km.`
  );

  if (relevantRouteDistanceKm > maxAllowedKmForRule) {
    console.warn(
      `[checkRouteDeviation] ROUTE DEVIATION EXCEEDED. Allowed: ${maxAllowedKmForRule}km, Actual route: ${relevantRouteDistanceKm.toFixed(3)}km. Returning false.`
    );
    return false;
  }

  console.log(`[checkRouteDeviation] Deviation OK. Returning true.`);
  return true;
}


async function calculateRouteDetails(
  routeCoordinates, // Expects [[lat, lng], ...] from your JS code
  employees, // Already in the desired sequence (from heuristic or OR-Tools)
  pickupTimePerEmployee, // This parameter is not used in the provided snippet, but kept for signature consistency
  tripType = "pickup",
  city
) {
  let osrmBaseUrl;
  if (city === "ncr") {
    osrmBaseUrl = "http://3.108.58.254:5000";
  } else if (city === "chennai") {
    osrmBaseUrl = "http://13.235.89.143:5000";
  } else {
    console.error(
      `[calculateRouteDetails] Unknown city: ${city}. Cannot determine OSRM base URL.`
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
      error: `Unknown city for OSRM URL: ${city}`,
    };
  }

  // Assuming TRAFFIC_BUFFER_PERCENTAGE, fetchApi, decodePolyline, encodePolyline are defined elsewhere
  const TRAFFIC_BUFFER_PERCENTAGE =
    typeof global !== "undefined" &&
    global.TRAFFIC_BUFFER_PERCENTAGE !== undefined
      ? global.TRAFFIC_BUFFER_PERCENTAGE
      : 0.1; // Default if not found

  try {
    if (!routeCoordinates || routeCoordinates.length === 0) {
      console.warn(
        "calculateRouteDetails: routeCoordinates is empty or null."
      );
      if (
        (!employees || employees.length === 0) &&
        routeCoordinates && // Added null check for routeCoordinates
        routeCoordinates.length > 1 // This condition was problematic, keeping structure but noting it
      ) {
        throw new Error(
          "Invalid input: routeCoordinates has multiple points but employees array is empty/null for /route call."
        );
      } else if (!employees && (!routeCoordinates || routeCoordinates.length === 0)) {
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
      // OSRM /route requires at least two coordinates
      if (routeCoordinates && routeCoordinates.length < 2) {
          throw new Error(
            "Invalid input: routeCoordinates must contain at least two points for an OSRM /route call."
          );
      }
    }


    const coordinatesString = routeCoordinates
      .map((c) => {
        if (
          typeof c[1] !== "number" ||
          typeof c[0] !== "number" ||
          isNaN(c[1]) || // Also check for NaN
          isNaN(c[0])
        ) {
          console.error(
            "Invalid coordinate pair in routeCoordinates for /route:",
            c
          );
          throw new Error(
            "Invalid coordinate pair found in routeCoordinates for /route."
          );
        }
        return `${c[1]},${c[0]}`; // lng,lat
      })
      .join(";");

    // Query parameters for /route
    const queryParams = "steps=true&geometries=polyline&overview=full";
    const url = `${osrmBaseUrl}/route/v1/driving/${coordinatesString}?${queryParams}`;

    // console.log(`[OSRM /route] Requesting: ${url}`);

    const response = await fetchApi(url, { method: "GET" }); // Assuming fetchApi is defined

    if (!response.ok) {
      const errorText = await response.text();
      console.error(
        `[OSRM /route] HTTP error: ${response.status} for URL: ${url}. Body: ${errorText}`
      );
      throw new Error(`OSRM /route error: ${response.status}`);
    }

    const data = await response.json();

    if (data.code !== "Ok" || !data.routes || data.routes.length === 0) {
      console.error(
        "[OSRM /route] API returned non-Ok code or no routes:",
        data,
        ` for URL: ${url}`
      );
      throw new Error(
        `Invalid OSRM /route response: ${data.code || "Unknown code"}, Message: ${data.message || "No routes found"}`
      );
    }

    const routeObject = data.routes[0]; // OSRM /route returns a 'routes' array
    const waypointsFromApi = data.waypoints;
    let fullRoadPolyline = routeObject.geometry || "";

    if (!fullRoadPolyline && routeObject.legs) {
      let fullCoords = [];
      for (const leg of routeObject.legs) {
        if (leg.steps) {
          for (const step of leg.steps) {
            const coords = decodePolyline(step.geometry); // Assuming decodePolyline is defined
            if (fullCoords.length > 0 && coords.length > 0) {
              const lastPt = fullCoords[fullCoords.length - 1];
              if (
                lastPt && // Ensure lastPt is defined
                coords[0] && // Ensure coords[0] is defined
                lastPt[0] === coords[0][0] &&
                lastPt[1] === coords[0][1]
              )
                fullCoords.pop();
            }
            fullCoords = fullCoords.concat(coords);
          }
        }
      }
      fullRoadPolyline = encodePolyline(fullCoords); // Assuming encodePolyline is defined
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

      for (const wp of waypointsFromApi) {
        const originalInputCoordIndex = wp.waypoint_index;
        const employeeForThisWaypoint = inputCoordIndexToEmployeeMap.get(
          originalInputCoordIndex
        );

        if (employeeForThisWaypoint) {
          orderedEmployees.push({
            ...employeeForThisWaypoint,
            order: orderedEmployees.length + 1,
          });
        }
      }

      if (orderedEmployees.length !== inputEmployeesOriginal.length) {
        console.warn(
          `[calculateRouteDetails OSRM /route] Mismatch after mapping waypoints. Expected ${inputEmployeesOriginal.length}, got ${orderedEmployees.length}. Using original employee order.`
        );
        // Fallback to original order if mapping is inconsistent
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
      totalDuration: routeObject.duration * (1 + TRAFFIC_BUFFER_PERCENTAGE),
      encodedPolyline: fullRoadPolyline,
      legs: routeObject.legs || [],
      geometry: {
        type: "LineString",
        coordinates: decodePolyline(fullRoadPolyline).map((c) => [c[1], c[0]]), // Polyline coords are [lat,lng], GeoJSON needs [lng,lat]
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
      facilityLocation,
      city
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
        facilityData.profile?.directionPenaltyWeight || 1.0,
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
                return null; // Should not happen if mapping is correct
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

// Helper: Get current unrouted employees respecting attempt counts
const getCurrentUnroutedEmployees = (
  unroutedEmployeesMasterList, // Pass this in
  routedEmployeeCodes, // Pass this in
  unroutableAttemptCounts, // Pass this in
  maxAttempts // Use this parameter
) => {
  return unroutedEmployeesMasterList.filter(
    (emp) =>
      emp.location && // Ensure employee has a location to be routable
      !routedEmployeeCodes.has(emp.empCode) &&
      (unroutableAttemptCounts.get(emp.empCode) || 0) < maxAttempts
  );
};

// Helper: Increment failed attempts
const incrementFailedAttempts = (
  employeeBatch,
  unroutableAttemptCounts, // Pass this in
  reason,
  maxAttempts // Use this parameter
) => {
  if (!employeeBatch || employeeBatch.length === 0) return;
  employeeBatch.forEach((emp) => {
    if (!emp || !emp.empCode) {
      // console.warn("[incrementFailedAttempts] Received invalid employee object in batch.");
      return;
    }
    const currentAttempts =
      (unroutableAttemptCounts.get(emp.empCode) || 0) + 1;
    unroutableAttemptCounts.set(emp.empCode, currentAttempts);
    // console.log( // Keep logging concise for main flow, enable for deep debug
    //   `[generateRoutesHelper] Emp ${emp.empCode} attempt ${currentAttempts}/${maxAttempts} failed: ${reason}.`
    // );
    if (currentAttempts >= maxAttempts) {
      console.log(
        `[generateRoutes] Emp ${emp.empCode} reached max ${maxAttempts} attempts for current phase (Reason: ${reason}).`
      );
    }
  });
};
// Helper: Check if any profiled fleet has capacity
const profiledFleetHasCapacity = (availableFleet) => { // Pass this in
  for (const typeKey in availableFleet) {
    if (availableFleet[typeKey].count > 0) return true;
  }
  return false;
};

async function generateDistanceDurationMatrix(
  locationsForMatrix, // Array of employee objects with .location {lat, lng}
  facilityLocation,
  city // {lat, lng}
  // No newApiBaseUrl or city needed as it's hardcoded to the OSRM-style endpoint
) {
  // --- Define the OSRM base URL here ---
  let osrmBaseUrl;
  // --- Define the OSRM base URL here ---
  if(city === "ncr"){
     osrmBaseUrl = "http://3.108.58.254:5000"; // Your specified OSRM server
  }
  else if(city === "chennai"){
    osrmBaseUrl  = "http://13.235.89.143:5000";
  }
  // ---

  // Input `locationsForMatrix` and `facilityLocation` are expected to have {lat, lng}
  const allPointsLatLon = [ // Array of {lat, lng}
    facilityLocation,
    ...locationsForMatrix.map((emp) => emp.location),
  ];

  if (allPointsLatLon.length <= 1) {
    return { distanceMatrix: [[]], durationMatrix: [[]], pointMap: [] };
  }

  // OSRM /table expects coordinates as {longitude},{latitude};{longitude},{latitude}
  const coordinatesString = allPointsLatLon
    .map((p) => {
      if (!p || typeof p.lng !== 'number' || typeof p.lat !== 'number') {
        console.error("[MatrixGen OSRM] Invalid point structure in allPointsLatLon:", p);
        throw new Error("Invalid point structure for OSRM matrix generation.");
      }
      return `${p.lng},${p.lat}`; // lng,lat
    })
    .join(";");

  const matrixTimeout = OSRM_PROBE_TIMEOUT + allPointsLatLon.length * 200; // Existing timeout logic
  const annotations = "duration,distance"; // Ensure both are requested
  const osrmTableUrl = `${osrmBaseUrl}/table/v1/driving/${coordinatesString}?annotations=${annotations}`;

  try {
    console.log(
      `[MatrixGen OSRM] Calling: ${osrmTableUrl} with ${allPointsLatLon.length} coordinates.`
    );
    const response = await fetchApi(osrmTableUrl, {
      method: "GET", // OSRM /table is a GET request
      timeout: matrixTimeout,
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(
        `[MatrixGen OSRM /table] HTTP error: ${response.status} ${response.statusText}. URL: ${osrmTableUrl}. Body: ${errorText}`
      );
      throw new Error(
        `OSRM /table service error: ${response.status} ${response.statusText}`
      );
    }

    const data = await response.json();

    // Standard OSRM /table response check
    if (data.code !== "Ok" || !data.durations || !data.distances) {
      console.error(
        "[MatrixGen OSRM /table] API returned non-Ok code or missing distances/durations:",
        data,
        `URL: ${osrmTableUrl}`
      );
      throw new Error(
        `Invalid OSRM /table response: Code ${data.code}, Message: ${data.message || "Missing distance/duration data"}`
      );
    }

    const pointMap = [
      { empCode: "FACILITY", isFacility: true, ...facilityLocation }, // Facility is at index 0
      ...locationsForMatrix, // Employees start from index 1
    ];

    return {
      distanceMatrix: data.distances, // Expecting 2D array [[row0], [row1], ...]
      durationMatrix: data.durations, // Expecting 2D array
      pointMap: pointMap,
    };
  } catch (error) {
    console.error(
      `[MatrixGen OSRM /table] Failed to generate distance/duration matrix with ${allPointsLatLon.length} points:`,
      error.message
    );
    // Log the URL for debugging if an error occurs
    if (!(error instanceof SyntaxError) && !(error.message.includes("service error"))) {
        console.error(`[MatrixGen OSRM /table] URL that caused error: ${osrmTableUrl}`);
    }
    throw error; // Rethrow to ensure the calling function knows about the failure.
  }
}


async function solveZoneWithORTools(
  zoneEmployees,
  facilityData,
  vehicleCapacities,
  maxRouteDurationSeconds,
  pickupTimePerEmployee,
  tripType,
  zoneName,
  forceSingleVehicleOptimization = false,
  vehicleTypes = [],
  city
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
    `\n[OR-TOOLS SOLVER] Solving for zone: "${currentZoneNameForLogging}" with ${zoneEmployees.length} employees. VehCap: ${vehicleCapacities}, MaxRouteDur: ${maxRouteDurationSeconds}s, PickupTime: ${pickupTimePerEmployee}s.`
  );

  const facilityLocation = { lat: facilityData.geoY, lng: facilityData.geoX };
  let pointMapForCurrentZone = [];

  try {
    const matrixData = await generateDistanceDurationMatrix(
      zoneEmployees,
      facilityLocation,
      city
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
      vehicle_capacities: vehicleCapacities,
      demands: demands,
      depot_index: 0,
      max_route_duration: maxRouteDurationSeconds,
      service_times: serviceTimes,
      allow_dropping_visits:
        facilityData.profile?.allowDroppingVisitsForProblematicZones || true,
      // Increase drop visit penalty to be high enough relative to distance in meters
      drop_visit_penalty: facilityData.profile?.dropPenalty || 5000000, // Increased default
      facility_coords: [facilityLocation.lat, facilityLocation.lng],
      trip_type: tripType.toUpperCase(),
      direction_penalty_weight:
        facilityData.profile?.directionPenaltyWeight || 1.0,
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
          solution.routes.forEach((route) => {
            if (route.node_indices && route.node_indices.length > 0) {
              const currentRouteEmployees = route.node_indices
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
                  vehicleCapacity: vehicleCapacities[0],
                  guardNeeded: false,
                  zone: currentZoneNameForLogging,
                  tripType,
                  vehicleType: vehicleTypes[0] || null,
                  vehicleIndex: route.vehicle_index || 0,
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
  // --- 1. Initialization ---
  const {
    employees,
    facility,
    shiftTime,
    date,
    profile,
    saveToDatabase = false,
    pickupTimePerEmployee = 180,
    guard = false,
    tripType = "PICKUP",
  } = data;

  if (!employees?.length) throw new Error("Employee data is required");
  if (!facility?.geoX || !facility?.geoY)
    throw new Error("Valid facility data required");
  if (!date || !shiftTime || !profile)
    throw new Error("Missing required parameters");

  const city  = profile?.name || "Unknown";

  const osrmAvailable = await isOsrmAvailable(city);
  if (!osrmAvailable) throw new Error("OSRM routing service unavailable");

  const employeesWithLocation = employees
    .map((emp) => ({
      ...emp,
      geoY: parseFloat(emp.geoY),
      geoX: parseFloat(emp.geoX),
      location:
        emp.geoY != null &&
        emp.geoX != null &&
        !isNaN(parseFloat(emp.geoY)) &&
        !isNaN(parseFloat(emp.geoX))
          ? {
              lat: parseFloat(emp.geoY),
              lng: parseFloat(emp.geoX),
            }
          : null,
      isMedical: emp.isMedical || false,
      isPWD: emp.isPWD || false,
    }))
    .filter((emp) => emp.location != null);

  const profileFleetConfig = buildFleetFromProfile(profile);
  const vehicleSpecsMaster = {
    s: { type: "s", capacity: 3 },
    m: { type: "m", capacity: 5 },
    l: { type: "l", capacity: 8 },
  };

  const availableFleet = {};
  if (profileFleetConfig && Array.isArray(profileFleetConfig)) {
    profileFleetConfig.forEach((v) => {
      if (v && v.type && vehicleSpecsMaster[v.type]) {
        if (
          v.count == null ||
          typeof v.count !== "number" ||
          isNaN(v.count)
        ) {
          console.error(
            `[generateRoutes] Vehicle type "${v.type}" from profile has invalid 'count'. Defaulting to 0.`
          );
          availableFleet[v.type] = {
            count: 0,
            spec: { ...vehicleSpecsMaster[v.type] },
          };
        } else {
          availableFleet[v.type] = {
            count: v.count,
            spec: { ...vehicleSpecsMaster[v.type] },
          };
        }
      } else {
        console.warn(
          `[generateRoutes] Invalid or unknown vehicle type in profileFleetConfig:`,
          v
        );
      }
    });
  }

  let allFinalRoutes = [];
  const unroutedEmployeesMasterList = [...employeesWithLocation];
  const routedEmployeeCodes = new Set();
  const unroutableAttemptCounts = new Map();
  const MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE =
    profile.maxRoutingAttemptsPerEmployee || 5;
  const profileMaxDuration = profile?.maxDuration || 7200;
  let finalTotalSwappedRoutes = 0;

  const facilityLocation = { lat: facility.geoY, lng: facility.geoX };
  // Assuming isSpecialNeedsUser is globally available or imported
  // const isSpecialNeedsUser = (emp) => (emp.isMedical || false) || (emp.isPWD || false);

  // --- 2. Primary Routing Loop (Using Profiled Fleet) ---
  console.log("\n--- Starting Primary Routing with Profiled Fleet ---");
  let primaryRoutingContinue = true;
  while (
    getCurrentUnroutedEmployees(
      unroutedEmployeesMasterList,
      routedEmployeeCodes,
      unroutableAttemptCounts,
      MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
    ).length > 0 &&
    profiledFleetHasCapacity(availableFleet) &&
    primaryRoutingContinue
  ) {
    const currentIterationUnrouted = getCurrentUnroutedEmployees(
      unroutedEmployeesMasterList,
      routedEmployeeCodes,
      unroutableAttemptCounts,
      MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
    );
    if (currentIterationUnrouted.length === 0) break;

    const largestCapacityInFleet =
      Object.keys(availableFleet).length > 0
        ? Math.max(
            ...Object.values(availableFleet)
              .filter((v) => v.count > 0)
              .map((v) => v.spec.capacity)
          )
        : vehicleSpecsMaster["l"]?.capacity || 8;

    const potentialHeuristicBatch =
      await heuristicallySelectEmployeesForVehicle(
        currentIterationUnrouted,
        largestCapacityInFleet,
        facility,
        tripType,
        profileMaxDuration,
        pickupTimePerEmployee,
        guard,
        profile
      );

    // --- START: HYBRID APPROACH - Iterative Trimming for Primary Loop ---
    let batchForPrimaryOrTools = potentialHeuristicBatch;

    if (batchForPrimaryOrTools && batchForPrimaryOrTools.length > 0) {
      let preliminaryDeviationOkay = false;
      let attemptsToMakeBatchCompliant = 0;
      const MAX_TRIM_ATTEMPTS = batchForPrimaryOrTools.length;
      let trimmedBatchForCheck = [...batchForPrimaryOrTools];

      while (
        trimmedBatchForCheck.length > 0 &&
        attemptsToMakeBatchCompliant < MAX_TRIM_ATTEMPTS
      ) {
        const tempHeuristicCoords = (
          tripType.toLowerCase() === "dropoff"
            ? [[facilityLocation.lat, facilityLocation.lng]]
            : []
        )
          .concat(
            trimmedBatchForCheck.map((e) => [e.location.lat, e.location.lng])
          )
          .concat(
            tripType.toLowerCase() === "pickup"
              ? [[facilityLocation.lat, facilityLocation.lng]]
              : []
          );

        const tempHeuristicRouteDetails = await calculateRouteDetails(
          tempHeuristicCoords,
          trimmedBatchForCheck,
          pickupTimePerEmployee,
          tripType,
          city
        );

        if (tempHeuristicRouteDetails.error) {
          console.warn(
            `[Hybrid Pre-Check Primary] OSRM error for heuristic batch (size ${trimmedBatchForCheck.length}). Error: ${tempHeuristicRouteDetails.error}. Discarding batch.`
          );
          batchForPrimaryOrTools = [];
          break;
        }

        const tempRouteForDeviationCheck = {
          employees: trimmedBatchForCheck,
          routeDetails: tempHeuristicRouteDetails,
          tripType: tripType,
          routeNumber: `H_PRE_CHECK_P_${allFinalRoutes.length}_${attemptsToMakeBatchCompliant}`,
        };

        preliminaryDeviationOkay = await checkRouteDeviation(
          tempRouteForDeviationCheck,
          facility,
          profile
        );

        if (preliminaryDeviationOkay) {
          console.log(
            `[Hybrid Pre-Check Primary] Heuristic batch (size ${trimmedBatchForCheck.length}) PASSED preliminary deviation.`
          );
          batchForPrimaryOrTools = [...trimmedBatchForCheck];
          break;
        } else {
          console.warn(
            `[Hybrid Pre-Check Primary] Heuristic batch (size ${trimmedBatchForCheck.length}) FAILED preliminary deviation. Trimming...`
          );
          if (trimmedBatchForCheck.length <= 1) {
            batchForPrimaryOrTools = [];
            break;
          }
          const removedEmp = trimmedBatchForCheck.pop();
          console.log(
            `[Hybrid Pre-Check Primary] Removed ${
              removedEmp?.empCode || "N/A"
            } during trim.`
          );
        }
        attemptsToMakeBatchCompliant++;
      }

      if (!preliminaryDeviationOkay && batchForPrimaryOrTools.length > 0) {
        console.warn(
          `[Hybrid Pre-Check Primary] Heuristic batch still failed deviation after ${attemptsToMakeBatchCompliant} trims. Discarding.`
        );
        batchForPrimaryOrTools = [];
      }

      if (
        batchForPrimaryOrTools.length === 0 &&
        potentialHeuristicBatch &&
        potentialHeuristicBatch.length > 0
      ) {
        console.warn(
          `[Hybrid Pre-Check Primary] Original heuristic batch (size ${potentialHeuristicBatch.length}) discarded due to pre-check failures.`
        );
        incrementFailedAttempts(
          potentialHeuristicBatch,
          unroutableAttemptCounts,
          "heuristic batch failed pre-OR-Tools deviation check (Primary)",
          MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
        );
      }
    }
    // --- END: HYBRID APPROACH - Iterative Trimming for Primary Loop ---

    if (!batchForPrimaryOrTools || batchForPrimaryOrTools.length === 0) {
      primaryRoutingContinue = false;
      continue;
    }

    let assignedVehicleKey = null;
    const batchSize = batchForPrimaryOrTools.length;
    const availableVehicleTypesSorted = Object.keys(availableFleet)
      .filter((typeKey) => availableFleet[typeKey].count > 0)
      .sort(
        (a, b) =>
          availableFleet[a].spec.capacity - availableFleet[b].spec.capacity
      );

    for (const typeKey of availableVehicleTypesSorted) {
      if (batchSize <= availableFleet[typeKey].spec.capacity) {
        assignedVehicleKey = typeKey;
        break;
      }
    }
    if (!assignedVehicleKey && availableVehicleTypesSorted.length > 0) {
      assignedVehicleKey =
        availableVehicleTypesSorted[availableVehicleTypesSorted.length - 1];
    }

    if (!assignedVehicleKey) {
      incrementFailedAttempts(
        batchForPrimaryOrTools,
        unroutableAttemptCounts,
        "no profiled vehicle for batch size after pre-check (Primary)",
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }

    const currentVehicleSpecToUse = availableFleet[assignedVehicleKey].spec;
    const baseVehicleEmployeeCapacity = currentVehicleSpecToUse.capacity;
    const employeesForOrTools = batchForPrimaryOrTools.slice(
      0,
      baseVehicleEmployeeCapacity
    );

    if (employeesForOrTools.length === 0) {
      incrementFailedAttempts(
        batchForPrimaryOrTools,
        unroutableAttemptCounts,
        "empty batch after slice (Primary, post-pre-check)",
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }
    console.log(
      `\n[generateRoutes] Primary: Batch of ${batchForPrimaryOrTools.length} (using ${employeesForOrTools.length} after pre-check & slice) for ${currentVehicleSpecToUse.type} (Cap: ${baseVehicleEmployeeCapacity}).`
    );

    let orSolutionPrimary;
    try {
      orSolutionPrimary = await solveZoneWithORTools(
        employeesForOrTools,
        facility,
        [baseVehicleEmployeeCapacity],
        profileMaxDuration,
        pickupTimePerEmployee,
        tripType,
        `P_BATCH_${allFinalRoutes.length}`,
        true,
        [currentVehicleSpecToUse.type],
        city
      );
      if (!orSolutionPrimary?.routes?.[0]?.employees) {
        incrementFailedAttempts(
          employeesForOrTools,
          unroutableAttemptCounts,
          "OR-Tools no solution or no employees array (Primary)",
          MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
        );
        continue;
      }
    } catch (orError) {
      incrementFailedAttempts(
        employeesForOrTools,
        unroutableAttemptCounts,
        `OR-Tools exception (Primary): ${orError.message}`,
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }

    const optimizedEmployeesFromSolverPrimary =
      orSolutionPrimary.routes[0].employees;
    if (
      !optimizedEmployeesFromSolverPrimary ||
      optimizedEmployeesFromSolverPrimary.length === 0
    ) {
      incrementFailedAttempts(
        employeesForOrTools,
        unroutableAttemptCounts,
        "OR-Tools returned no employees in route (Primary)",
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }

    const validOptimizedEmployees =
      optimizedEmployeesFromSolverPrimary.filter((emp) => {
        if (
          emp &&
          emp.location &&
          emp.location.lat != null &&
          emp.location.lng != null
        )
          return true;
        incrementFailedAttempts(
          [emp],
          unroutableAttemptCounts,
          "OR-Tools output emp missing location (Primary)",
          MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
        );
        return false;
      });

    if (validOptimizedEmployees.length === 0) {
      incrementFailedAttempts(
        employeesForOrTools,
        unroutableAttemptCounts,
        "All OR-Tools output emps invalid location (Primary)",
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }

    let currentRouteDetails = await calculateRouteDetails(
      (tripType.toLowerCase() === "dropoff"
        ? [[facilityLocation.lat, facilityLocation.lng]]
        : []
      )
        .concat(
          validOptimizedEmployees.map((e) => [e.location.lat, e.location.lng])
        )
        .concat(
          tripType.toLowerCase() === "pickup"
            ? [[facilityLocation.lat, facilityLocation.lng]]
            : []
        ),
      validOptimizedEmployees,
      pickupTimePerEmployee,
      tripType,
      city
    );
    if (currentRouteDetails.error) {
      incrementFailedAttempts(
        validOptimizedEmployees,
        unroutableAttemptCounts,
        `OSRM after OR-Tools (Primary): ${currentRouteDetails.error}`,
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }

    let tempRouteForCheck = {
      employees: validOptimizedEmployees,
      routeDetails: currentRouteDetails,
      tripType: tripType,
      routeNumber: `P_R_CHECK_${allFinalRoutes.length}`,
    };
    const initialDeviationOkay = await checkRouteDeviation(
      tempRouteForCheck,
      facility,
      profile
    );
    const initialDurationOkay =
      currentRouteDetails.totalDuration <= profileMaxDuration;

    if (!initialDeviationOkay || !initialDurationOkay) {
      incrementFailedAttempts(
        validOptimizedEmployees,
        unroutableAttemptCounts,
        `Initial Dev/Dur Fail (Primary) (Dev: ${initialDeviationOkay}, Dur: ${initialDurationOkay})`,
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }

    let finalRoute = {
      routeNumber: allFinalRoutes.length + 1,
      employees: [...validOptimizedEmployees],
      vehicleType: currentVehicleSpecToUse.type,
      vehicleCapacity: baseVehicleEmployeeCapacity,
      zone: validOptimizedEmployees[0]?.zone || "ASSIGNED_ZONE_P",
      tripType: tripType.toUpperCase(),
      uniqueKey: `${currentVehicleSpecToUse.type}_${
        allFinalRoutes.length
      }_${uuidv4()}`,
      routeDetails: currentRouteDetails,
      encodedPolyline: currentRouteDetails.encodedPolyline,
      swapped: false,
      durationExceeded: !initialDurationOkay,
      isSpecialNeedsRoute: validOptimizedEmployees.some((e) =>
        isSpecialNeedsUser(e)
      ),
      guardNeeded: false,
      afterFleetExhaustion: false,
    };

    if (
      guard &&
      finalRoute.employees.length > 0 
      // isNightShiftForGuard(shiftTime, tripType, profile)
    ) {
      const isDropoffForGuard = tripType.toLowerCase() === "dropoff";
      const guardResult = await handleGuardRequirements(
        finalRoute,
        isDropoffForGuard,
        facility,
        pickupTimePerEmployee,
        city
      );

      if (
        guardResult.swapped &&
        guardResult.routeDetails &&
        !guardResult.routeDetails.error
      ) {
        updateRouteWithDetails(finalRoute, guardResult.routeDetails);
        finalRoute.swapped = true;
        finalTotalSwappedRoutes++;
        finalRoute.guardNeeded = false;
        // --- POTENTIAL CALL SITE ---
    console.log(`[generateRoutes] Route ${finalRoute.routeNumber} was swapped. Attempting re-optimization.`);
    const reOptResults = await reOptimizeSwappedRouteWithORTools(
      finalRoute, // The route object after swap
      facility,   // Facility data (includes profile)
      pickupTimePerEmployee,
      city // Pass city if generateDistanceDurationMatrix inside needs it
    );
    if (reOptResults.reOptimized && reOptResults.employees && reOptResults.employees.length > 0) {
      // Recalculate details for the re-optimized route
      const reOptimizedCoords = (tripType.toLowerCase() === "dropoff" ? [[facilityLocation.lat, facilityLocation.lng]] : [])
          .concat(reOptResults.employees.map(e => [e.location.lat, e.location.lng]))
          .concat(tripType.toLowerCase() === "pickup" ? [[facilityLocation.lat, facilityLocation.lng]] : []);
    
      const reOptimizedDetails = await calculateRouteDetails(
          reOptimizedCoords, reOptResults.employees, pickupTimePerEmployee, tripType, city
      );
      if (!reOptimizedDetails.error) {
          console.log(`[generateRoutes] Route ${finalRoute.routeNumber} successfully re-optimized after swap.`);
          finalRoute.employees = reOptResults.employees;
          updateRouteWithDetails(finalRoute, reOptimizedDetails);
      } else {
          console.warn(`[generateRoutes] Route ${finalRoute.routeNumber} re-optimization OSRM call failed. Using original swapped route.`);
      }
    } else if (reOptResults.error) {
       console.warn(`[generateRoutes] Re-optimization for swapped route ${finalRoute.routeNumber} failed: ${reOptResults.error}`);
    }
    // --- END POTENTIAL CALL SITE ---
      } else if (guardResult.guardNeeded) {
        finalRoute.guardNeeded = true;
      } else {
        finalRoute.guardNeeded = false;
      }
    }

    let removedEmployeeForGuard = null;
    let effectiveEmployeeCapacityForVehicle = baseVehicleEmployeeCapacity;
    if (finalRoute.guardNeeded) {
      effectiveEmployeeCapacityForVehicle = baseVehicleEmployeeCapacity - 1;
      if (
        finalRoute.employees.length > effectiveEmployeeCapacityForVehicle &&
        effectiveEmployeeCapacityForVehicle >= 0
      ) {
        if (finalRoute.employees.length > 0) {
          removedEmployeeForGuard = finalRoute.employees.pop();
          console.log(
            `[generateRoutes Primary] Emp ${
              removedEmployeeForGuard?.empCode
            } removed from Route ${
              finalRoute.routeNumber
            } for guard space.`
          );

          if (finalRoute.employees.length > 0) {
            const validEmployeesAfterPop = finalRoute.employees.filter(
              (emp) => emp.location
            );
            finalRoute.employees = validEmployeesAfterPop;

            if (finalRoute.employees.length === 0) {
              if (removedEmployeeForGuard)
                incrementFailedAttempts(
                  [removedEmployeeForGuard],
                  unroutableAttemptCounts,
                  "Empty after guard removal (post-filter in Primary)",
                  MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
                );
              continue;
            }

            const newRouteDetails = await calculateRouteDetails(
              (tripType.toLowerCase() === "dropoff"
                ? [[facilityLocation.lat, facilityLocation.lng]]
                : []
              )
                .concat(
                  finalRoute.employees.map((e) => [
                    e.location.lat,
                    e.location.lng,
                  ])
                )
                .concat(
                  tripType.toLowerCase() === "pickup"
                    ? [[facilityLocation.lat, facilityLocation.lng]]
                    : []
                ),
              finalRoute.employees,
              pickupTimePerEmployee,
              tripType,
              city
            );
            if (newRouteDetails.error) {
              incrementFailedAttempts(
                finalRoute.employees,
                unroutableAttemptCounts,
                `OSRM recalc fail after guard (Primary): ${newRouteDetails.error}`,
                MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
              );
              if (removedEmployeeForGuard)
                incrementFailedAttempts(
                  [removedEmployeeForGuard],
                  unroutableAttemptCounts,
                  "OSRM recalc fail (removed emp in Primary)",
                  MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
                );
              continue;
            }
            updateRouteWithDetails(finalRoute, newRouteDetails);
            tempRouteForCheck = { ...finalRoute };
            const reDeviationOkay = await checkRouteDeviation(
              tempRouteForCheck,
              facility,
              profile
            );
            const reDurationOkay =
              finalRoute.routeDetails.totalDuration <= profileMaxDuration;
            if (!reDeviationOkay || !reDurationOkay) {
              incrementFailedAttempts(
                finalRoute.employees,
                unroutableAttemptCounts,
                `Re-Dev/Dur Fail after guard (Primary) (Dev: ${reDeviationOkay}, Dur: ${reDurationOkay})`,
                MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
              );
              if (removedEmployeeForGuard)
                incrementFailedAttempts(
                  [removedEmployeeForGuard],
                  unroutableAttemptCounts,
                  "Re-Dev/Dur Fail (removed emp in Primary)",
                  MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
                );
              continue;
            }
          } else {
            if (removedEmployeeForGuard)
              incrementFailedAttempts(
                [removedEmployeeForGuard],
                unroutableAttemptCounts,
                "Empty after guard removal (pop in Primary)",
                MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
              );
            continue;
          }
        }
      } else if (
        finalRoute.employees.length > effectiveEmployeeCapacityForVehicle &&
        effectiveEmployeeCapacityForVehicle < 0
      ) {
        console.warn(
          `[generateRoutes Primary] Guard needed but vehicle capacity ${baseVehicleEmployeeCapacity} too small. Route ${finalRoute.routeNumber} cannot be formed.`
        );
        incrementFailedAttempts(
          finalRoute.employees,
          unroutableAttemptCounts,
          "Guard needed, vehicle capacity too small (Primary)",
          MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
        );
        continue;
      }
    }
    if (
      finalRoute.employees.length === 0 &&
      !(
        finalRoute.guardNeeded &&
        baseVehicleEmployeeCapacity === 1 &&
        effectiveEmployeeCapacityForVehicle === 0
      )
    ) {
      if (
        !removedEmployeeForGuard &&
        validOptimizedEmployees.length > 0
      ) {
        incrementFailedAttempts(
          validOptimizedEmployees,
          unroutableAttemptCounts,
          "Route became empty before adding (Primary)",
          MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
        );
      }
      continue;
    }
    if (
      finalRoute.employees.length > effectiveEmployeeCapacityForVehicle &&
      effectiveEmployeeCapacityForVehicle >= 0
    ) {
      incrementFailedAttempts(
        finalRoute.employees,
        unroutableAttemptCounts,
        "Exceeds effective employee capacity (Primary)",
        MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE
      );
      continue;
    }

    if (finalRoute.employees.length > 1) {
      const routeBeforePolish = JSON.parse(JSON.stringify(finalRoute));
      const polishingInput = {
        employees: [...finalRoute.employees],
        vehicleCapacity: finalRoute.vehicleCapacity,
        tripType: finalRoute.tripType,
        vehicleType: finalRoute.vehicleType,
        fixedStartNodeForPolish:
          finalRoute.swapped && tripType.toUpperCase() === "PICKUP" ? 1 : null,
      };
      const polishResult = await polishRouteWithORTools(
        polishingInput,
        facility,
        pickupTimePerEmployee,
        profile
      );
      if (
        polishResult.polished &&
        polishResult.employees &&
        polishResult.routeDetails &&
        !polishResult.routeDetails.error
      ) {
        const tempPolishedRouteCheck = {
          employees: polishResult.employees,
          routeDetails: polishResult.routeDetails,
          tripType: finalRoute.tripType,
          routeNumber: finalRoute.routeNumber,
        };
        const polishedDeviationOkay = await checkRouteDeviation(
          tempPolishedRouteCheck,
          facility,
          profile
        );
        const polishedDurationOkay =
          polishResult.routeDetails.totalDuration <= profileMaxDuration;

        if (polishedDeviationOkay && polishedDurationOkay) {
          finalRoute.employees = polishResult.employees;
          updateRouteWithDetails(finalRoute, polishResult.routeDetails);
          finalRoute.durationExceeded = !polishedDurationOkay;
          console.log(
            `[generateRoutes Primary] Route ${finalRoute.routeNumber} polished and re-validated.`
          );
          if (
            guard &&
            finalRoute.employees.length > 0 
            // isNightShiftForGuard(shiftTime, tripType, profile)
          ) {
            const isDropoffForGuard = tripType.toLowerCase() === "dropoff";
            const guardResultAfterPolish = await handleGuardRequirements(
              finalRoute,
              isDropoffForGuard,
              facility,
              pickupTimePerEmployee,
              city
            );
            if (
              guardResultAfterPolish.swapped &&
              guardResultAfterPolish.routeDetails &&
              !guardResultAfterPolish.routeDetails.error
            ) {
              updateRouteWithDetails(
                finalRoute,
                guardResultAfterPolish.routeDetails
              );
              finalRoute.swapped = true;
              if (!routeBeforePolish.swapped) finalTotalSwappedRoutes++;
              finalRoute.guardNeeded = false;
            } else if (guardResultAfterPolish.guardNeeded) {
              finalRoute.guardNeeded = true;
            } else {
              finalRoute.guardNeeded = false;
            }
          }
        } else {
          console.warn(
            `[generateRoutes Primary] Route ${finalRoute.routeNumber} FAILED dev/dur after POLISHING (Dev: ${polishedDeviationOkay}, Dur: ${polishedDurationOkay}). Reverting.`
          );
        }
      } else if (polishResult.error) {
        console.warn(
          `[generateRoutes Primary] Polishing error for route ${finalRoute.routeNumber}: ${polishResult.error}`
        );
      }
    }

    if (finalRoute.employees.length > 0) {
      const farthestEmployee =
        tripType.toLowerCase() === "pickup"
          ? finalRoute.employees[0]
          : finalRoute.employees[finalRoute.employees.length - 1];
      const farthestRoutePoints =
        tripType.toLowerCase() === "pickup"
          ? [
              [farthestEmployee.location.lat, farthestEmployee.location.lng],
              [facilityLocation.lat, facilityLocation.lng],
            ]
          : [
              [facilityLocation.lat, facilityLocation.lng],
              [farthestEmployee.location.lat, farthestEmployee.location.lng]
            ];

      const farthestRouteDetails = await calculateRouteDetails(
        farthestRoutePoints,
        [farthestEmployee],
        pickupTimePerEmployee,
        tripType,
        city
      );
      if (farthestRouteDetails.error) {
        finalRoute.farthestEmployeeDistance = 0;
      } else {
        finalRoute.farthestEmployeeDistance =
          farthestRouteDetails.totalDistance;
      }
      calculatePickupTimes(
        finalRoute,
        shiftTime,
        pickupTimePerEmployee,
        profile?.reportingTimeSeconds || 0
      );
    } else {
      finalRoute.farthestEmployeeDistance = 0;
    }

    allFinalRoutes.push(finalRoute);
    finalRoute.employees.forEach((emp) =>
      routedEmployeeCodes.add(emp.empCode)
    );
    if (removedEmployeeForGuard) {
      routedEmployeeCodes.delete(removedEmployeeForGuard.empCode);
    }
    availableFleet[assignedVehicleKey].count--;
    console.log(
      `  SUCCESS (Profiled Fleet): Routed ${
        finalRoute.employees.length
      } emps (Guard: ${finalRoute.guardNeeded}) in ${
        finalRoute.vehicleType
      }. ${
        availableFleet[assignedVehicleKey].count
      } of type ${assignedVehicleKey} left.`
    );
  }

  // --- 3. Default Vehicle Routing Loop ---
  console.log(
    "\n--- Starting Default Vehicle Routing for Remaining Employees ---"
  );
  const defaultRoutingMaxAttempts = MAX_ROUTING_ATTEMPTS_PER_EMPLOYEE + 2;

  if (
    getCurrentUnroutedEmployees(
      unroutedEmployeesMasterList,
      routedEmployeeCodes,
      unroutableAttemptCounts,
      defaultRoutingMaxAttempts
    ).length > 0
  ) {
    const defaultVehicleSpecToUse = vehicleSpecsMaster["m"] || {
      type: "m",
      capacity: 5,
    };
    const defaultBaseEmployeeCapacityForRoute =
      defaultVehicleSpecToUse.capacity;
    console.log(
      `[generateRoutes] ${
        getCurrentUnroutedEmployees(
          unroutedEmployeesMasterList,
          routedEmployeeCodes,
          unroutableAttemptCounts,
          defaultRoutingMaxAttempts
        ).length
      } emps remaining. Using default ${
        defaultVehicleSpecToUse.type
      } (Base Emp Cap: ${defaultBaseEmployeeCapacityForRoute}).`
    );

    let defaultRouteMadeInLoopIteration = true;
    while (
      getCurrentUnroutedEmployees(
        unroutedEmployeesMasterList,
        routedEmployeeCodes,
        unroutableAttemptCounts,
        defaultRoutingMaxAttempts
      ).length > 0 &&
      defaultRouteMadeInLoopIteration
    ) {
      defaultRouteMadeInLoopIteration = false;
      const currentDefaultUnrouted = getCurrentUnroutedEmployees(
        unroutedEmployeesMasterList,
        routedEmployeeCodes,
        unroutableAttemptCounts,
        defaultRoutingMaxAttempts
      );
      if (currentDefaultUnrouted.length === 0) break;

      const defaultHeuristicBatch =
        await heuristicallySelectEmployeesForVehicle(
          currentDefaultUnrouted,
          defaultBaseEmployeeCapacityForRoute,
          facility,
          tripType,
          profileMaxDuration,
          pickupTimePerEmployee,
          guard,
          profile
        );

        if (tripType.toLowerCase() === "dropoff" || tripType.toLowerCase() === "d") {
          console.log(
            `[DEBUG DefaultLoop - ${tripType}] Initial batch from HSFEV: ${
              defaultHeuristicBatch
                ? `Size ${defaultHeuristicBatch.length}, EmpCodes: ${defaultHeuristicBatch
                    .map((e) => e.empCode)
                    .join(",")}`
                : "NULL or EMPTY"
            }`
          );
        }
        // ***** END ADDED LOG *****

      // --- START: HYBRID APPROACH - Iterative Trimming for Default Loop ---
      let batchForDefaultOrTools = defaultHeuristicBatch;

      if (batchForDefaultOrTools && batchForDefaultOrTools.length > 0) {
        let preliminaryDeviationOkay = false;
        let attemptsToMakeBatchCompliant = 0;
        const MAX_TRIM_ATTEMPTS = batchForDefaultOrTools.length;
        let trimmedBatchForCheck = [...batchForDefaultOrTools];

        while (
          trimmedBatchForCheck.length > 0 &&
          attemptsToMakeBatchCompliant < MAX_TRIM_ATTEMPTS
        ) {
          const tempHeuristicCoords = (
            tripType.toLowerCase() === "dropoff"
              ? [[facilityLocation.lat, facilityLocation.lng]]
              : []
          )
            .concat(
              trimmedBatchForCheck.map((e) => [e.location.lat, e.location.lng])
            )
            .concat(
              tripType.toLowerCase() === "pickup"
                ? [[facilityLocation.lat, facilityLocation.lng]]
                : []
            );
          const tempHeuristicRouteDetails = await calculateRouteDetails(
            tempHeuristicCoords,
            trimmedBatchForCheck,
            pickupTimePerEmployee,
            tripType,
            city
          );

          if (tempHeuristicRouteDetails.error) {
            console.warn(
              `[Hybrid Pre-Check Default] OSRM error for heuristic batch (size ${trimmedBatchForCheck.length}). Error: ${tempHeuristicRouteDetails.error}. Discarding batch.`
            );
            batchForDefaultOrTools = [];
            break;
          }

          const tempRouteForDeviationCheck = {
            employees: trimmedBatchForCheck,
            routeDetails: tempHeuristicRouteDetails,
            tripType: tripType,
            routeNumber: `H_PRE_CHECK_D_${allFinalRoutes.length}_${attemptsToMakeBatchCompliant}`,
          };
          preliminaryDeviationOkay = await checkRouteDeviation(
            tempRouteForDeviationCheck,
            facility,
            profile
          );

          if (preliminaryDeviationOkay) {
            console.log(
              `[Hybrid Pre-Check Default] Heuristic batch (size ${trimmedBatchForCheck.length}) PASSED preliminary deviation.`
            );
            batchForDefaultOrTools = [...trimmedBatchForCheck];
            break;
          } else {
            console.warn(
              `[Hybrid Pre-Check Default] Heuristic batch (size ${trimmedBatchForCheck.length}) FAILED preliminary deviation. Trimming...`
            );
            if (trimmedBatchForCheck.length <= 1) {
              batchForDefaultOrTools = [];
              break;
            }
            const removedEmp = trimmedBatchForCheck.pop();
            console.log(
              `[Hybrid Pre-Check Default] Removed ${
                removedEmp?.empCode || "N/A"
              } during trim.`
            );
          }
          attemptsToMakeBatchCompliant++;
        }
        if (!preliminaryDeviationOkay && batchForDefaultOrTools.length > 0) {
          console.warn(
            `[Hybrid Pre-Check Default] Heuristic batch still failed deviation after ${attemptsToMakeBatchCompliant} trims. Discarding.`
          );
          batchForDefaultOrTools = [];
        }
        if (
          batchForDefaultOrTools.length === 0 &&
          defaultHeuristicBatch &&
          defaultHeuristicBatch.length > 0
        ) {
          console.warn(
            `[Hybrid Pre-Check Default] Original heuristic batch (size ${defaultHeuristicBatch.length}) discarded due to pre-check failures.`
          );
          incrementFailedAttempts(
            defaultHeuristicBatch,
            unroutableAttemptCounts,
            "heuristic batch failed pre-OR-Tools deviation check (Default)",
            defaultRoutingMaxAttempts
          );
        }
      }
      // --- END: HYBRID APPROACH - Iterative Trimming for Default Loop ---

      if (!batchForDefaultOrTools || batchForDefaultOrTools.length === 0) {
        console.log(
          `[generateRoutes DefaultLoop - ${tripType}] Heuristic (even after trim) couldn't form a usable batch. Ending default attempts for this cycle.`
        );
        if (currentDefaultUnrouted.length > 0 && (!batchForDefaultOrTools || batchForDefaultOrTools.length === 0)) {
          const failedBatchAttempt = currentDefaultUnrouted.slice(0, defaultBaseEmployeeCapacityForRoute); // The ones HSFEV likely considered
          incrementFailedAttempts(
              failedBatchAttempt,
              unroutableAttemptCounts,
              "HSFEV failed to form batch in default loop",
              defaultRoutingMaxAttempts
          );
      }
      continue;
      }

      const employeesForDefaultOrTools = batchForDefaultOrTools;
      console.log(
        `\n[generateRoutes DefaultLoop] Batch of ${
          employeesForDefaultOrTools.length
        } (after pre-check) for ${defaultVehicleSpecToUse.type} (Cap: ${
          defaultBaseEmployeeCapacityForRoute
        }).`
      );

      let orSolutionDefault;
      try {
        orSolutionDefault = await solveZoneWithORTools(
          employeesForDefaultOrTools,
          facility,
          [defaultBaseEmployeeCapacityForRoute],
          profileMaxDuration,
          pickupTimePerEmployee,
          tripType,
          `D_BATCH_${allFinalRoutes.length}`,
          true,
          [defaultVehicleSpecToUse.type],
          city
        );
        if (!orSolutionDefault?.routes?.[0]?.employees) {
          incrementFailedAttempts(
            employeesForDefaultOrTools,
            unroutableAttemptCounts,
            "Default OR-Tools no solution or no employees array",
            defaultRoutingMaxAttempts
          );
          continue;
        }
      } catch (orError) {
        incrementFailedAttempts(
          employeesForDefaultOrTools,
          unroutableAttemptCounts,
          `Default OR-Tools exception: ${orError.message}`,
          defaultRoutingMaxAttempts
        );
        continue;
      }

      const defaultOptimizedEmployeesFromSolver =
        orSolutionDefault.routes[0].employees;
      if (
        !defaultOptimizedEmployeesFromSolver ||
        defaultOptimizedEmployeesFromSolver.length === 0
      ) {
        incrementFailedAttempts(
          employeesForDefaultOrTools,
          unroutableAttemptCounts,
          "Default OR-Tools returned no employees in route",
          defaultRoutingMaxAttempts
        );
        continue;
      }

      const validDefaultOptimizedEmployees =
        defaultOptimizedEmployeesFromSolver.filter((emp) => {
          if (
            emp &&
            emp.location &&
            emp.location.lat != null &&
            emp.location.lng != null
          )
            return true;
          incrementFailedAttempts(
            [emp],
            unroutableAttemptCounts,
            "Default OR-Tools output emp missing location",
            defaultRoutingMaxAttempts
          );
          return false;
        });

      if (validDefaultOptimizedEmployees.length === 0) {
        incrementFailedAttempts(
          employeesForDefaultOrTools,
          unroutableAttemptCounts,
          "All Default OR-Tools output emps invalid location",
          defaultRoutingMaxAttempts
        );
        continue;
      }

      let defaultRouteDetails = await calculateRouteDetails(
        (tripType.toLowerCase() === "dropoff"
          ? [[facilityLocation.lat, facilityLocation.lng]]
          : []
        )
          .concat(
            validDefaultOptimizedEmployees.map((e) => [
              e.location.lat,
              e.location.lng,
            ])
          )
          .concat(
            tripType.toLowerCase() === "pickup"
              ? [[facilityLocation.lat, facilityLocation.lng]]
              : []
          ),
        validDefaultOptimizedEmployees,
        pickupTimePerEmployee,
        tripType,
        city
      );
      if (defaultRouteDetails.error) {
        incrementFailedAttempts(
          validDefaultOptimizedEmployees,
          unroutableAttemptCounts,
          `Default OSRM Error: ${defaultRouteDetails.error}`,
          defaultRoutingMaxAttempts
        );
        continue;
      }

      let tempDefaultRouteCheck = {
        employees: validDefaultOptimizedEmployees,
        routeDetails: defaultRouteDetails,
        tripType: tripType,
        routeNumber: `D_R_CHECK_${allFinalRoutes.length}`,
      };
      const defaultDeviationOkay = await checkRouteDeviation(
        tempDefaultRouteCheck,
        facility,
        profile
      );
      const defaultDurationOkay =
        defaultRouteDetails.totalDuration <= profileMaxDuration;

      if (!defaultDeviationOkay || !defaultDurationOkay) {
        incrementFailedAttempts(
          validDefaultOptimizedEmployees,
          unroutableAttemptCounts,
          `Default Dev/Dur Fail (Dev: ${defaultDeviationOkay}, Dur: ${defaultDurationOkay})`,
          defaultRoutingMaxAttempts
        );
        continue;
      }

      let finalDefaultRoute = {
        routeNumber: allFinalRoutes.length + 1,
        employees: [...validDefaultOptimizedEmployees],
        vehicleType: defaultVehicleSpecToUse.type,
        vehicleCapacity: defaultBaseEmployeeCapacityForRoute,
        zone: validDefaultOptimizedEmployees[0]?.zone || "DEFAULT_ZONE_D",
        tripType: tripType.toUpperCase(),
        uniqueKey: `DEFAULT_${defaultVehicleSpecToUse.type}_${
          allFinalRoutes.length
        }_${uuidv4()}`,
        routeDetails: defaultRouteDetails,
        encodedPolyline: defaultRouteDetails.encodedPolyline,
        swapped: false,
        durationExceeded: !defaultDurationOkay,
        isSpecialNeedsRoute: validDefaultOptimizedEmployees.some((e) =>
          isSpecialNeedsUser(e)
        ),
        guardNeeded: false,
        afterFleetExhaustion: true,
      };

      if (
        guard &&
        finalDefaultRoute.employees.length > 0 
        // isNightShiftForGuard(shiftTime, tripType, profile)
      ) {
        const isDropoffForGuardDefault = tripType.toLowerCase() === "dropoff";
        const guardResultDefault = await handleGuardRequirements(
          finalDefaultRoute,
          isDropoffForGuardDefault,
          facility,
          pickupTimePerEmployee,
          city
        );

        if (
          guardResultDefault.swapped &&
          guardResultDefault.routeDetails &&
          !guardResultDefault.routeDetails.error
        ) {
          updateRouteWithDetails(
            finalDefaultRoute,
            guardResultDefault.routeDetails
          );
          finalDefaultRoute.swapped = true;
          finalTotalSwappedRoutes++;
          finalDefaultRoute.guardNeeded = false;
        } else if (guardResultDefault.guardNeeded) {
          finalDefaultRoute.guardNeeded = true;
        } else {
          finalDefaultRoute.guardNeeded = false;
        }
      }

      let removedEmpForDefaultGuard = null;
      let defaultEffectiveEmpCapacity = defaultBaseEmployeeCapacityForRoute;
      if (finalDefaultRoute.guardNeeded) {
        defaultEffectiveEmpCapacity = defaultBaseEmployeeCapacityForRoute - 1;
        if (
          finalDefaultRoute.employees.length > defaultEffectiveEmpCapacity &&
          defaultEffectiveEmpCapacity >= 0
        ) {
          if (finalDefaultRoute.employees.length > 0) {
            removedEmpForDefaultGuard = finalDefaultRoute.employees.pop();
            console.log(
              `[generateRoutes DefaultLoop] Emp ${
                removedEmpForDefaultGuard?.empCode
              } removed for guard space.`
            );
            if (finalDefaultRoute.employees.length > 0) {
              const validDefaultEmployeesAfterPop =
                finalDefaultRoute.employees.filter((e) => e.location);
              finalDefaultRoute.employees = validDefaultEmployeesAfterPop;

              if (finalDefaultRoute.employees.length === 0) {
                if (removedEmpForDefaultGuard)
                  incrementFailedAttempts(
                    [removedEmpForDefaultGuard],
                    unroutableAttemptCounts,
                    "Empty after guard removal (Default)",
                    defaultRoutingMaxAttempts
                  );
                continue;
              }

              const newDefaultRouteDetails = await calculateRouteDetails(
                (tripType.toLowerCase() === "dropoff"
                  ? [[facilityLocation.lat, facilityLocation.lng]]
                  : []
                )
                  .concat(
                    finalDefaultRoute.employees.map((e) => [
                      e.location.lat,
                      e.location.lng,
                    ])
                  )
                  .concat(
                    tripType.toLowerCase() === "pickup"
                      ? [[facilityLocation.lat, facilityLocation.lng]]
                      : []
                  ),
                finalDefaultRoute.employees,
                pickupTimePerEmployee,
                tripType,
                city
              );
              if (newDefaultRouteDetails.error) {
                incrementFailedAttempts(
                  finalDefaultRoute.employees,
                  unroutableAttemptCounts,
                  `Default OSRM recalc fail: ${newDefaultRouteDetails.error}`,
                  defaultRoutingMaxAttempts
                );
                if (removedEmpForDefaultGuard)
                  incrementFailedAttempts(
                    [removedEmpForDefaultGuard],
                    unroutableAttemptCounts,
                    "Default OSRM recalc (removed)",
                    defaultRoutingMaxAttempts
                  );
                continue;
              }
              updateRouteWithDetails(
                finalDefaultRoute,
                newDefaultRouteDetails
              );
              tempDefaultRouteCheck = { ...finalDefaultRoute };
              const reDeviationOkayDefault = await checkRouteDeviation(
                tempDefaultRouteCheck,
                facility,
                profile
              );
              const reDurationOkayDefault =
                finalDefaultRoute.routeDetails.totalDuration <=
                profileMaxDuration;
              if (!reDeviationOkayDefault || !reDurationOkayDefault) {
                incrementFailedAttempts(
                  finalDefaultRoute.employees,
                  unroutableAttemptCounts,
                  `Default Re-Dev/Dur (Dev: ${reDeviationOkayDefault}, Dur: ${reDurationOkayDefault})`,
                  defaultRoutingMaxAttempts
                );
                if (removedEmpForDefaultGuard)
                  incrementFailedAttempts(
                    [removedEmpForDefaultGuard],
                    unroutableAttemptCounts,
                    "Default Re-Dev/Dur (removed)",
                    defaultRoutingMaxAttempts
                  );
                continue;
              }
            } else {
              if (removedEmpForDefaultGuard)
                incrementFailedAttempts(
                  [removedEmpForDefaultGuard],
                  unroutableAttemptCounts,
                  "Default empty after guard removal (pop)",
                  defaultRoutingMaxAttempts
                );
              continue;
            }
          }
        } else if (
          finalDefaultRoute.employees.length > defaultEffectiveEmpCapacity &&
          defaultEffectiveEmpCapacity < 0
        ) {
          console.warn(
            `[generateRoutes DefaultLoop] Guard needed but vehicle capacity ${defaultBaseEmployeeCapacityForRoute} too small. Route ${finalDefaultRoute.routeNumber} cannot be formed.`
          );
          incrementFailedAttempts(
            finalDefaultRoute.employees,
            unroutableAttemptCounts,
            "Guard needed, vehicle capacity too small (Default)",
            defaultRoutingMaxAttempts
          );
          continue;
        }
      }

      if (
        finalDefaultRoute.employees.length === 0 &&
        !(
          finalDefaultRoute.guardNeeded &&
          defaultBaseEmployeeCapacityForRoute === 1 &&
          defaultEffectiveEmpCapacity === 0
        )
      ) {
        if (
          !removedEmpForDefaultGuard &&
          validDefaultOptimizedEmployees.length > 0
        ) {
          incrementFailedAttempts(
            validDefaultOptimizedEmployees,
            unroutableAttemptCounts,
            "Default route became empty",
            defaultRoutingMaxAttempts
          );
        }
        continue;
      }
      if (
        finalDefaultRoute.employees.length > defaultEffectiveEmpCapacity &&
        defaultEffectiveEmpCapacity >= 0
      ) {
        incrementFailedAttempts(
          finalDefaultRoute.employees,
          unroutableAttemptCounts,
          "Default exceeds effective employee capacity",
          defaultRoutingMaxAttempts
        );
        continue;
      }

      if (finalDefaultRoute.employees.length > 1) {
        const routeBeforePolishDefault = JSON.parse(
          JSON.stringify(finalDefaultRoute)
        );
        const polishingInputDefault = {
          employees: [...finalDefaultRoute.employees],
          vehicleCapacity: finalDefaultRoute.vehicleCapacity,
          tripType: finalDefaultRoute.tripType,
          vehicleType: finalDefaultRoute.vehicleType,
        };
        const polishResultDefault = await polishRouteWithORTools(
          polishingInputDefault,
          facility,
          pickupTimePerEmployee,
          profile
        );
        if (
          polishResultDefault.polished &&
          polishResultDefault.employees &&
          polishResultDefault.routeDetails &&
          !polishResultDefault.routeDetails.error
        ) {
          const tempPolishedDefaultRouteCheck = {
            employees: polishResultDefault.employees,
            routeDetails: polishResultDefault.routeDetails,
            tripType: finalDefaultRoute.tripType,
            routeNumber: finalDefaultRoute.routeNumber,
          };
          const polishedDeviationOkayDefault = await checkRouteDeviation(
            tempPolishedDefaultRouteCheck,
            facility,
            profile
          );
          const polishedDurationOkayDefault =
            polishResultDefault.routeDetails.totalDuration <=
            profileMaxDuration;

          if (polishedDeviationOkayDefault && polishedDurationOkayDefault) {
            finalDefaultRoute.employees = polishResultDefault.employees;
            updateRouteWithDetails(
              finalDefaultRoute,
              polishResultDefault.routeDetails
            );
            finalDefaultRoute.durationExceeded = !polishedDurationOkayDefault;
            console.log(
              `[generateRoutes DefaultLoop] Route ${finalDefaultRoute.routeNumber} polished and re-validated.`
            );
            if (
              guard &&
              finalDefaultRoute.employees.length > 0 
              // isNightShiftForGuard(shiftTime, tripType, profile)
            ) {
              const isDropoffForGuard = tripType.toLowerCase() === "dropoff";
              const guardResultAfterPolish = await handleGuardRequirements(
                finalDefaultRoute,
                isDropoffForGuard,
                facility,
                pickupTimePerEmployee,
                city
              );
              if (
                guardResultAfterPolish.swapped &&
                guardResultAfterPolish.routeDetails &&
                !guardResultAfterPolish.routeDetails.error
              ) {
                updateRouteWithDetails(
                  finalDefaultRoute,
                  guardResultAfterPolish.routeDetails
                );
                finalDefaultRoute.swapped = true;
                if (!routeBeforePolishDefault.swapped)
                  finalTotalSwappedRoutes++;
                finalDefaultRoute.guardNeeded = false;
              } else if (guardResultAfterPolish.guardNeeded) {
                finalDefaultRoute.guardNeeded = true;
              } else {
                finalDefaultRoute.guardNeeded = false;
              }
            }
          } else {
            console.warn(
              `[generateRoutes DefaultLoop] Route ${finalDefaultRoute.routeNumber} FAILED dev/dur after POLISHING (Dev: ${polishedDeviationOkayDefault}, Dur: ${polishedDurationOkayDefault}). Reverting.`
            );
          }
        } else if (polishResultDefault.error) {
          console.warn(
            `[generateRoutes DefaultLoop] Polishing error for route ${finalDefaultRoute.routeNumber}: ${polishResultDefault.error}`
          );
        }
      }

      if (finalDefaultRoute.employees.length > 0) {
        const farthestEmployeeDefault =
          tripType.toLowerCase() === "pickup"
            ? finalDefaultRoute.employees[0]
            : finalDefaultRoute.employees[
                finalDefaultRoute.employees.length - 1
              ];
        const farthestRoutePointsDefault =
          tripType.toLowerCase() === "pickup"
            ? [
                [
                  farthestEmployeeDefault.location.lat,
                  farthestEmployeeDefault.location.lng,
                ],
                [facilityLocation.lat, facilityLocation.lng],
              ]
            : [
                [
                  farthestEmployeeDefault.location.lat,
                  farthestEmployeeDefault.location.lng,
                ],
                [facilityLocation.lat, facilityLocation.lng],
              ];

        const farthestRouteDetailsDefault = await calculateRouteDetails(
          farthestRoutePointsDefault,
          [farthestEmployeeDefault],
          pickupTimePerEmployee,
          tripType,
          city
        );
        if (farthestRouteDetailsDefault.error) {
          finalDefaultRoute.farthestEmployeeDistance = 0;
        } else {
          finalDefaultRoute.farthestEmployeeDistance =
            farthestRouteDetailsDefault.totalDistance;
        }
        calculatePickupTimes(
          finalDefaultRoute,
          shiftTime,
          pickupTimePerEmployee,
          profile?.reportingTimeSeconds || 0
        );
      } else {
        finalDefaultRoute.farthestEmployeeDistance = 0;
      }

      allFinalRoutes.push(finalDefaultRoute);
      finalDefaultRoute.employees.forEach((emp) =>
        routedEmployeeCodes.add(emp.empCode)
      );
      if (removedEmpForDefaultGuard) {
        routedEmployeeCodes.delete(removedEmpForDefaultGuard.empCode);
      }
      defaultRouteMadeInLoopIteration = true;
      console.log(
        `  SUCCESS (Default Fleet): Routed ${
          finalDefaultRoute.employees.length
        } emps (Guard: ${finalDefaultRoute.guardNeeded}) in ${
          defaultVehicleSpecToUse.type
        }.`
      );
    }
  }

  // --- 4. Finalization ---
  const routeData = {
    uuid: data.uuid || uuidv4(),
    date: data.date,
    shift: shiftTime,
    tripType: tripType.toUpperCase(),
    facility: facility,
    profile: profile,
    employeeData: employeesWithLocation,
    routeData: allFinalRoutes,
  };
  const stats = calculateRouteStatistics(routeData, employees.length);

  const response = createSimplifiedResponse({
    ...routeData,
    ...stats,
    totalSwappedRoutes: finalTotalSwappedRoutes,
  });

  response.unroutedEmployees = employeesWithLocation
    .filter((emp) => !routedEmployeeCodes.has(emp.empCode))
    .map((emp) => ({
      empCode: emp.empCode,
      geoX: emp.geoX,
      geoY: emp.geoY,
      gender: emp.gender,
      isMedical: emp.isMedical || false,
      isPWD: emp.isPWD || false,
      location: emp.location,
    }));

  console.log(
    `\n[generateRoutes] COMPLETED. Total Routes: ${
      response.routes.length
    }. Routed: ${response.totalRoutedEmployees}. Unrouted: ${
      response.unroutedEmployees.length
    }.`
  );
  return response;
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

async function polishRouteWithORTools(
  routeToPolish, // Object containing { employees, vehicleCapacity, tripType, vehicleType, fixedStartNodeForPolish, fixedEndNodeForPolish, otherCustomersForFixedEndPolish }
  facilityData,
  pickupTimePerEmployee,
  profile // For maxDuration, directionPenaltyWeight etc.
) {
  const {
    employees: currentEmployees,
    vehicleCapacity, // Base employee capacity of the assigned vehicle
    tripType,
    vehicleType, // For logging or if OR-Tools needs it
    fixedStartNodeForPolish, // e.g., 1 if the first employee is fixed (PICKUP)
    fixedEndNodeForPolish, // e.g., currentEmployees.length if the last is fixed (DROPOFF)
    otherCustomersForFixedEndPolish, // Array of other customer indices for fixed end (DROPOFF)
  } = routeToPolish;

  const profileMaxDuration = profile?.maxDuration || 7200;
  const city  = profile?.name || "Unknown City"; 

  if (!currentEmployees || currentEmployees.length === 0) {
    return {
      polished: false,
      employees: currentEmployees,
      error: "No employees to polish",
    };
  }
  if (currentEmployees.length === 1 && !fixedStartNodeForPolish && !fixedEndNodeForPolish) {
    // OR-Tools won't change the order of a single employee route unless a fix is forced
    // (though fixing a single node route is trivial)
    return {
      polished: true,
      employees: currentEmployees,
      routeDetails: routeToPolish.routeDetails,
    };
  }

  console.log(
    `\n[POLISH OR-TOOLS] Polishing route with ${
      currentEmployees.length
    } employees for ${vehicleType} (Cap: ${vehicleCapacity}). Trip: ${tripType}`
  );
  if (fixedStartNodeForPolish) {
    console.log(`  Fixed Start Node for Polish: ${fixedStartNodeForPolish}`);
  }
  if (fixedEndNodeForPolish) {
    console.log(`  Fixed End Node for Polish: ${fixedEndNodeForPolish}`);
    if (otherCustomersForFixedEndPolish) {
      console.log(`  Other customers for Fixed End: ${otherCustomersForFixedEndPolish.join(',')}`);
    }
  }


  const facilityLocation = {
    lat: facilityData.geoY,
    lng: facilityData.geoX,
  };

  try {
    const matrixData = await generateDistanceDurationMatrix(
      currentEmployees,
      facilityLocation,
      city
    );
    const { distanceMatrix, durationMatrix, pointMap } = matrixData;

    if (
      !distanceMatrix ||
      distanceMatrix.length === 0 ||
      (distanceMatrix.length > 0 && distanceMatrix[0].length === 0)
    ) {
      console.warn(
        `[POLISH OR-TOOLS] Empty/invalid matrix for polishing. Skipping.`
      );
      return {
        polished: false,
        employees: currentEmployees,
        error: "Matrix generation failed",
      };
    }
    if (pointMap.length !== distanceMatrix.length) {
      console.error(
        `[POLISH OR-TOOLS] Mismatch pointMap and matrix dimensions!`
      );
      return {
        polished: false,
        employees: currentEmployees,
        error: "Matrix dimension mismatch",
      };
    }

    const demands = [0, ...currentEmployees.map(() => 1)];
    const serviceTimes = [
      0,
      ...currentEmployees.map(() => pickupTimePerEmployee),
    ];

    const orToolsInput = {
      distance_matrix: distanceMatrix,
      duration_matrix: durationMatrix,
      num_vehicles: 1,
      vehicle_capacities: [vehicleCapacity],
      demands: demands,
      depot_index: 0, // Facility is at index 0 in the matrix
      max_route_duration: profileMaxDuration,
      service_times: serviceTimes,
      allow_dropping_visits: false,
      facility_coords: [facilityLocation.lat, facilityLocation.lng],
      trip_type: tripType.toUpperCase(),
      direction_penalty_weight: profile?.directionPenaltyWeight || 1.0,
    };

    // Add fixed node parameters to OR-Tools input if provided
    if (tripType.toUpperCase() === "PICKUP" && fixedStartNodeForPolish != null) {
      // fixedStartNodeForPolish is 1-based index of the customer in the currentEmployees list
      // This directly corresponds to their 1-based index in the Python solver's matrix (after depot)
      orToolsInput.fixed_start_node_index_in_matrix = fixedStartNodeForPolish;
      console.log(
        `  [POLISH OR-TOOLS] Applying fixed_start_node_index_in_matrix: ${fixedStartNodeForPolish}`
      );
    } else if (tripType.toUpperCase() === "DROPOFF" && fixedEndNodeForPolish != null) {
      // fixedEndNodeForPolish is 1-based index of the customer
      orToolsInput.fixed_end_node_index_in_matrix = fixedEndNodeForPolish;
      console.log(
        `  [POLISH OR-TOOLS] Applying fixed_end_node_index_in_matrix: ${fixedEndNodeForPolish}`
      );
      if (otherCustomersForFixedEndPolish && otherCustomersForFixedEndPolish.length > 0) {
        orToolsInput.other_customer_node_indices_in_matrix = otherCustomersForFixedEndPolish;
        console.log(
          `  [POLISH OR-TOOLS] Applying other_customer_node_indices_in_matrix: ${otherCustomersForFixedEndPolish.join(',')}`
        );
      }
    }

    const pythonExecutable = "python";
    const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py");

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
      console.error(`[POLISH OR-TOOLS Python stderr]: ${errData}`);
      scriptError += errData;
    });

    return new Promise((resolve) => {
      pythonProcess.on("close", async (code) => {
        let solution = null;
        try {
          const lines = scriptOutput.trim().split("\n");
          const lastLine = lines[lines.length - 1].trim();
          if (lastLine.startsWith("{") && lastLine.endsWith("}")) {
            solution = JSON.parse(lastLine);
          } else {
            throw new Error("No valid JSON from polishing script");
          }
        } catch (e) {
          console.error(
            `[POLISH OR-TOOLS] Error parsing Python output: ${e.message}. Raw: ${scriptOutput}`
          );
          return resolve({
            polished: false,
            employees: currentEmployees,
            error: "Python output parse error",
            routeDetails: routeToPolish.routeDetails, // Return original details
          });
        }

        if (code !== 0 || !solution || solution.error) {
          console.error(
            `[POLISH OR-TOOLS] Python script error or no solution. Code: ${code}, Solution Error: ${
              solution?.error
            }, Script Error: ${scriptError}`
          );
          return resolve({
            polished: false,
            employees: currentEmployees,
            error: solution?.error || `Python exit ${code}`,
            routeDetails: routeToPolish.routeDetails,
          });
        }
        if (
          solution.dropped_node_indices &&
          solution.dropped_node_indices.length > 0
        ) {
          console.warn(
            "[POLISH OR-TOOLS] Nodes were dropped during polishing, which shouldn't happen. Using original order."
          );
          return resolve({
            polished: false,
            employees: currentEmployees,
            error: "Nodes dropped during polishing",
            routeDetails: routeToPolish.routeDetails,
          });
        }

        if (
          solution.routes &&
          Array.isArray(solution.routes) &&
          solution.routes.length > 0 &&
          solution.routes[0].node_indices &&
          solution.routes[0].node_indices.length === currentEmployees.length
        ) {
          const polishedNodeIndices = solution.routes[0].node_indices;
          const polishedEmployeeList = polishedNodeIndices
            .map((nodeIdxInMatrix) => {
              if (
                nodeIdxInMatrix > 0 &&
                nodeIdxInMatrix <= currentEmployees.length
              ) {
                return currentEmployees[nodeIdxInMatrix - 1];
              }
              console.error(`[POLISH OR-TOOLS] Invalid node index ${nodeIdxInMatrix} from solver.`);
              return null;
            })
            .filter(Boolean);

          if (polishedEmployeeList.length !== currentEmployees.length) {
            console.warn(
              "[POLISH OR-TOOLS] Polished employee list length mismatch. Using original."
            );
            return resolve({
              polished: false,
              employees: currentEmployees,
              error: "Length mismatch after polishing",
              routeDetails: routeToPolish.routeDetails,
            });
          }
          
          // CRITICAL CHECK: If a node was fixed, ensure it's in the correct position
          if (tripType.toUpperCase() === "PICKUP" && fixedStartNodeForPolish != null) {
            if (polishedEmployeeList.length > 0 && 
                currentEmployees[fixedStartNodeForPolish - 1].empCode !== polishedEmployeeList[0].empCode) {
                console.warn(`[POLISH OR-TOOLS] Fixed start node for PICKUP was not honored by solver! Expected ${currentEmployees[fixedStartNodeForPolish - 1].empCode} but got ${polishedEmployeeList[0].empCode}. Reverting.`);
                return resolve({ polished: false, employees: currentEmployees, error: "Fixed start not honored", routeDetails: routeToPolish.routeDetails });
            }
          } else if (tripType.toUpperCase() === "DROPOFF" && fixedEndNodeForPolish != null) {
             if (polishedEmployeeList.length > 0 &&
                 currentEmployees[fixedEndNodeForPolish - 1].empCode !== polishedEmployeeList[polishedEmployeeList.length - 1].empCode) {
                 console.warn(`[POLISH OR-TOOLS] Fixed end node for DROPOFF was not honored by solver! Expected ${currentEmployees[fixedEndNodeForPolish - 1].empCode} but got ${polishedEmployeeList[polishedEmployeeList.length - 1].empCode}. Reverting.`);
                return resolve({ polished: false, employees: currentEmployees, error: "Fixed end not honored", routeDetails: routeToPolish.routeDetails });
             }
          }


          const routeCoordinates = polishedEmployeeList.map((emp) => [
            emp.location.lat,
            emp.location.lng,
          ]);
          const facilityCoordsArray = [
            facilityData.geoY,
            facilityData.geoX,
          ];
          const allCoordinatesForTrip =
            tripType.toLowerCase() === "dropoff"
              ? [facilityCoordsArray, ...routeCoordinates]
              : [...routeCoordinates, facilityCoordsArray];

          const newRouteDetails = await calculateRouteDetails(
            allCoordinatesForTrip,
            polishedEmployeeList,
            pickupTimePerEmployee,
            tripType,
            city
          );

          if (newRouteDetails.error) {
            console.warn(
              `[POLISH OR-TOOLS] OSRM recalculation failed after polishing. Using original. Error: ${newRouteDetails.error}`
            );
            return resolve({
              polished: false,
              employees: currentEmployees,
              error: "OSRM failed after polishing",
              routeDetails: routeToPolish.routeDetails,
            });
          }

          console.log(
            `[POLISH OR-TOOLS] Successfully polished route. New order length: ${polishedEmployeeList.length}`
          );
          resolve({
            polished: true,
            employees: polishedEmployeeList,
            routeDetails: newRouteDetails,
          });
        } else {
          console.warn(
            "[POLISH OR-TOOLS] OR-Tools returned no valid route or incorrect number of employees. Using original order."
          );
          resolve({
            polished: false,
            employees: currentEmployees,
            error: "No valid route from polishing",
            routeDetails: routeToPolish.routeDetails,
          });
        }
      });
      pythonProcess.on("error", (err) => {
        console.error(
          `[POLISH OR-TOOLS] Failed to start Python subprocess: ${err.message}`
        );
        resolve({
          polished: false,
          employees: currentEmployees,
          error: "Python spawn error",
          routeDetails: routeToPolish.routeDetails,
        });
      });
    });
  } catch (error) {
    console.error(
      `[POLISH OR-TOOLS] Critical error in polishRouteWithORTools: ${error.message}`
    );
    return {
      polished: false,
      employees: currentEmployees,
      error: error.message,
      routeDetails: routeToPolish.routeDetails,
    };
  }
}



// In generateRoutes.js

// Assuming OSRM_PROBE_TIMEOUT_HEURISTIC, MAX_SWAP_DISTANCE_KM are defined
// Assuming fetchApi is your fetch wrapper
// Assuming calculateRouteDetails is updated to use the new OSRM server

async function handleGuardRequirements(
  route, // Pass the original route object
  isDropoff,
  facility, // Contains facility.geoX, facility.geoY
  pickupTimePerEmployee,
  city
) {
  // --- Define the OSRM base URL here ---
  let osrmBaseUrl;
  // --- Define the OSRM base URL here ---
  if(city === "ncr"){
     osrmBaseUrl = "http://3.108.58.254:5000"; // Your specified OSRM server
  }
  else if(city === "chennai"){
    osrmBaseUrl  = "http://13.235.89.143:5000";
  }
  // ---

  try {
    const currentEmployees = [...route.employees];

    if (!currentEmployees?.length || currentEmployees.length < 1) {
      return { guardNeeded: false, swapped: false, routeDetails: route.routeDetails };
    }

    const checkIndex = isDropoff ? currentEmployees.length - 1 : 0;
    const criticalEmployee = currentEmployees[checkIndex];

    if (!criticalEmployee || !criticalEmployee.location || criticalEmployee.gender !== "F") {
      return { guardNeeded: false, swapped: false, routeDetails: route.routeDetails };
    }

    if (currentEmployees.length === 1 && criticalEmployee.gender === "F") {
      return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
    }

    const potentialMaleCandidates = currentEmployees.filter(
      (emp, index) => index !== checkIndex && emp.gender === "M" && emp.location
    );

    if (potentialMaleCandidates.length === 0) {
      return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
    }

    // Prepare coordinates for OSRM /table call
    // First coordinate is the criticalEmployee, followed by potentialMaleCandidates
    const osrmInputCoordinates = [
      criticalEmployee.location, // {lat, lng}
      ...potentialMaleCandidates.map((emp) => emp.location), // array of {lat, lng}
    ];

    const coordinatesString = osrmInputCoordinates
      .map((loc) => {
        if (!loc || typeof loc.lng !== 'number' || typeof loc.lat !== 'number') {
          console.error("[handleGuardRequirements] Invalid location object for OSRM call:", loc);
          // This indicates a data problem upstream if an employee in a route doesn't have a valid location
          throw new Error("Invalid location object found for guard requirement check.");
        }
        return `${loc.lng},${loc.lat}`;
      })
      .join(";");

    // For /table, sources=0 means the first coordinate (criticalEmployee)
    // destinations=1;2;3... means the subsequent coordinates (potentialMaleCandidates)
    const sourcesParam = "0";
    const destinationsParam = potentialMaleCandidates
      .map((_, i) => i + 1) // Indices relative to osrmInputCoordinates
      .join(";");
    const annotationsParam = "distance"; // We only need distances for this check

    const osrmTableUrl = `${osrmBaseUrl}/table/v1/driving/${coordinatesString}?sources=${sourcesParam}&destinations=${destinationsParam}&annotations=${annotationsParam}`;
    let osrmDistancesRow = []; // Will store the distances from criticalEmployee to males

    try {
      // console.log(`[handleGuardRequirements OSRM /table] Calling: ${osrmTableUrl}`);
      const response = await fetchApi(osrmTableUrl, {
        method: "GET",
        timeout: OSRM_PROBE_TIMEOUT_HEURISTIC,
      });

      if (response.ok) {
        const data = await response.json();
        // OSRM /table with sources/destinations returns distances as data.distances[source_index][destination_table_index]
        if (data.code === "Ok" && data.distances && data.distances.length > 0 && data.distances[0]) {
          osrmDistancesRow = data.distances[0]; // This is an array of distances from source 0 to all destinations
        } else {
          console.warn(
            `[handleGuardRequirements OSRM /table] API error for route ${
              route.routeNumber
            }: ${data.code} - ${data.message || "No distances returned"}`
          );
          return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
        }
      } else {
        const errorText = await response.text();
        console.warn(
          `[handleGuardRequirements OSRM /table] HTTP error ${response.status} for route ${route.routeNumber}. URL: ${osrmTableUrl}. Body: ${errorText}`
        );
        return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
      }
    } catch (error) {
      console.error(
        `[handleGuardRequirements OSRM /table] Fetch error for route ${route.routeNumber}:`,
        error.message,
        `URL: ${osrmTableUrl}`
      );
      return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
    }

    const validCandidates = [];
    potentialMaleCandidates.forEach((maleEmp, idx) => {
      // idx here corresponds to the order in potentialMaleCandidates,
      // and thus to the order in osrmDistancesRow
      const roadDistanceMeters = osrmDistancesRow[idx];
      if (roadDistanceMeters != null) {
        const roadDistanceKm = roadDistanceMeters / 1000;
        if (roadDistanceKm <= MAX_SWAP_DISTANCE_KM) {
          validCandidates.push({
            employee: maleEmp,
            indexInCurrentList: currentEmployees.findIndex(
              (e) => e.empCode === maleEmp.empCode
            ),
            distance: roadDistanceKm,
          });
        }
      }
    });

    if (validCandidates.length === 0) {
      return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
    }

    validCandidates.sort((a, b) => a.distance - b.distance);
    const bestCandidate = validCandidates[0];

    const swappedEmployeeList = [...currentEmployees];
    
    if (bestCandidate.indexInCurrentList === -1 || bestCandidate.indexInCurrentList === checkIndex) {
      console.error(
        `[handleGuardRequirements] Error finding original index for best candidate or candidate is the critical employee. Candidate EmpCode: ${bestCandidate.employee?.empCode}`
      );
      return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
    }

    const maleToSwapIn = swappedEmployeeList[bestCandidate.indexInCurrentList];
    const femaleToSwapOut = swappedEmployeeList[checkIndex];
    swappedEmployeeList[checkIndex] = maleToSwapIn;
    swappedEmployeeList[bestCandidate.indexInCurrentList] = femaleToSwapOut;
    
    const intendedCriticalPositionEmployee = maleToSwapIn;

    const newRouteCoordinates = swappedEmployeeList.map((emp) => [
      emp.location.lat,
      emp.location.lng,
    ]);
    const facilityCoordsArray = [facility.geoY, facility.geoX];
    
    const allCoordinatesForTrip = isDropoff
      ? [facilityCoordsArray, ...newRouteCoordinates]
      : [...newRouteCoordinates, facilityCoordsArray];

    // Assuming calculateRouteDetails is already updated to use the new OSRM server
    const routeDetailsAfterOsrmTrip = await calculateRouteDetails(
      allCoordinatesForTrip,
      swappedEmployeeList,
      pickupTimePerEmployee,
      route.tripType,
      city
    );

    if (routeDetailsAfterOsrmTrip.error) {
      console.warn(
        `[handleGuardRequirements] Swap validation (OSRM /route) failed for route ${route.routeNumber} after manual swap: ${routeDetailsAfterOsrmTrip.error}`
      );
      return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
    }

    if (!isDropoff) { // Pickup: Check if swapped male is still first
      if (routeDetailsAfterOsrmTrip.employees.length > 0 &&
          routeDetailsAfterOsrmTrip.employees[0].empCode !== intendedCriticalPositionEmployee.empCode) {
        console.warn(
          `[handleGuardRequirements] Route ${route.routeNumber}: OSRM reordered after swap. Intended first: ${intendedCriticalPositionEmployee.empCode}, OSRM first: ${routeDetailsAfterOsrmTrip.employees[0].empCode}. Attempting to force swapped order.`
        );
        
        const finalOrderedEmployees = [...routeDetailsAfterOsrmTrip.employees];
        const swappedMaleIndexInOsrmOrder = finalOrderedEmployees.findIndex(e => e.empCode === intendedCriticalPositionEmployee.empCode);
        
        if (swappedMaleIndexInOsrmOrder > 0) {
            const male = finalOrderedEmployees.splice(swappedMaleIndexInOsrmOrder, 1)[0];
            finalOrderedEmployees.unshift(male);

            const forcedOrderRouteCoordinates = finalOrderedEmployees.map(emp => [emp.location.lat, emp.location.lng]);
            const forcedAllCoordinates = [facilityCoordsArray, ...forcedOrderRouteCoordinates]; // Assuming pickup, facility at end
            if (tripType.toLowerCase() === "pickup") {
                 forcedAllCoordinates.shift(); // Remove facility from start
                 forcedAllCoordinates.push(facilityCoordsArray); // Add facility at end
            }
            
            const forcedRouteDetails = await calculateRouteDetails(
                forcedAllCoordinates,
                finalOrderedEmployees,
                pickupTimePerEmployee,
                route.tripType,
                city
            );

            if (forcedRouteDetails.error) {
                 console.warn(`[handleGuardRequirements] OSRM recalculation failed after forcing swapped order for route ${route.routeNumber}. Swap failed. Error: ${forcedRouteDetails.error}`);
                 return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
            }
            console.log(`[handleGuardRequirements] Route ${route.routeNumber}: Successfully forced swapped order with male first.`);
            return {
              guardNeeded: false,
              swapped: true,
              routeDetails: forcedRouteDetails,
            };
        } else if (swappedMaleIndexInOsrmOrder === -1) {
            console.error(`[handleGuardRequirements] Swapped male ${intendedCriticalPositionEmployee.empCode} missing from OSRM result for route ${route.routeNumber}. Swap failed.`);
            return { guardNeeded: true, swapped: false, routeDetails: route.routeDetails };
        }
      }
    }
    // Add similar check for dropoff if needed: if male is not last after OSRM reorder.
    // else if (isDropoff) {
    //    if (routeDetailsAfterOsrmTrip.employees.length > 0 &&
    //        routeDetailsAfterOsrmTrip.employees[routeDetailsAfterOsrmTrip.employees.length -1].empCode !== intendedCriticalPositionEmployee.empCode) {
    //          // Logic to force male to be last and recalculate
    //    }
    // }


    console.log(`[handleGuardRequirements] Route ${route.routeNumber}: Swap successful. OSRM order for swapped list:`, routeDetailsAfterOsrmTrip.employees.map(e=>e.empCode).join(','));
    return {
      guardNeeded: false,
      swapped: true,
      routeDetails: routeDetailsAfterOsrmTrip,
    };

  } catch (error) {
    console.error(
      `Error in handleGuardRequirements for route ${route?.routeNumber}:`,
      error.message,
      error.stack // Log stack for more details
    );
    return { guardNeeded: true, swapped: false, routeDetails: route?.routeDetails };
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
    `[Route ${route.routeNumber || "UNKNOWN"}] Assigning error state: ${message}` // Added logging
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
  if (!route || !routeDetails) {
     console.warn(`[Route ${route?.routeNumber || "UNKNOWN"}] updateRouteWithDetails called with invalid input. Route: ${!!route}, Details: ${!!routeDetails}`); // Added logging
     return;
  }
  if (routeDetails.error) {
    console.warn(
      `[Route ${route.routeNumber}] Not updating with errored details: ${routeDetails.error}` // Added logging
    );
    assignErrorState(
      route,
      `Failed to update with details: ${routeDetails.error}`
    );
    return;
  }
  console.log(`[Route ${route.routeNumber}] Updating with valid details.`); // Added logging
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
  let totalGuardedRoutes = 0; // Initialize the counter

  validRoutes.forEach((route) => {
    // Access totalDistance and totalDuration from route.routeDetails
    const routeDist = route.routeDetails?.totalDistance;
    const routeDur = route.routeDetails?.totalDuration;

    if (routeDur !== Infinity && routeDist !== Infinity) {
      totalDistanceSum += routeDist || 0;
      totalDurationSum += routeDur || 0;
    }

    // Check if the guardNeeded flag is true for the route
    if (route.guardNeeded === true) {
      totalGuardedRoutes++;
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
    totalGuardedRoutes: totalGuardedRoutes, // Add the new statistic
  };
}


// In generateRoutes.js

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
    totalGuardedRoutes: routeData.totalGuardedRoutes,
    routes: routeData.routeData
      .filter((route) => !route.error && route.employees?.length > 0)
      .map((route) => {
        const guardAssigned = route.guardNeeded || false;
        const occupancy = (route.employees?.length || 0);
        let vehicleType = route.vehicleType || null;
        if (!vehicleType && route.vehicleIndex != null && Array.isArray(route.vehicleTypes)) {
          vehicleType = route.vehicleTypes[route.vehicleIndex] || null;
        }

        const sortedEmployees = [...route.employees].sort((a, b) => (a.order || 0) - (b.order || 0));

        // Check for medical and PWD employees
        const hasMedicalEmployee = sortedEmployees.some(emp => emp.isMedical === true);
        const hasPWDEmployee = sortedEmployees.some(emp => emp.isPWD === true);


        return {
          routeNumber: route.routeNumber,
          zone: route.zone,
          vehicleCapacity: route.vehicleCapacity,
          vehicleType: vehicleType,
          guard: guardAssigned,
          swapped: route.swapped || false,
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
            ((route.farthestEmployeeDistance || 0) / 1000).toFixed(2)
          ),
          // Add new keys
          isMedicalRoute: hasMedicalEmployee,
          isPWDRoute: hasPWDEmployee,
          isNMTRoute: false,
          isOOBRoute: false,
          encodedPolyline: route.encodedPolyline || "no_polyline",
          employees: sortedEmployees.map((emp, index) => ({
            empCode: emp.empCode,
            gender: emp.gender,
            isMedical: emp.isMedical || false,
            isPWD: emp.isPWD || false,
            isNMT: false,
            isOOB: false,
            eta:
              route.tripType?.toUpperCase() === "DROPOFF"
                ? emp.dropoffTime
                : emp.pickupTime,
            order: emp.order || (index + 1),
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

function buildFleetFromProfile(profile) {
  if (!profile || !Array.isArray(profile.fleet)) {
    console.error(
      "[buildFleetFromProfile] Profile missing or profile.fleet is not an array. Returning empty fleet config."
    );
    return [];
  }

  // profile.fleet should be like:
  // [
  //   { type: "4-seater", capacity: 3, count: 82 },
  //   { type: "6/7-seater", capacity: 5, count: 378 },
  //   { type: "12-seater", capacity: 8, count: 105 }
  // ]
  // The function should just validate and return this structure.

  return profile.fleet
    .map((vehicleEntry) => {
      if (
        !vehicleEntry ||
        !vehicleEntry.type ||
        vehicleEntry.capacity == null || // check for null or undefined
        vehicleEntry.count == null     // check for null or undefined
      ) {
        console.warn(
          "[buildFleetFromProfile] Invalid vehicle entry in profile.fleet, skipping:",
          vehicleEntry
        );
        return null; // Mark for filtering
      }
      const capacity = parseInt(vehicleEntry.capacity, 10);
      const count = parseInt(vehicleEntry.count, 10);

      if (isNaN(capacity) || isNaN(count)) {
        console.warn(
          "[buildFleetFromProfile] Invalid capacity or count for vehicle type " + vehicleEntry.type + ", skipping:",
          vehicleEntry
        );
        return null; // Mark for filtering
      }
      return {
        type: vehicleEntry.type,
        capacity: capacity,
        count: count,
      };
    })
    .filter(Boolean); // Remove any null entries from map if validation failed
}


// Helper to ensure vehicleCapacities is always an array
function ensureArray(val) {
  return Array.isArray(val) ? val : [val];
}

// Utility function to chunk an array into smaller arrays of a given size
function chunkArray(array, chunkSize) {
  const results = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    results.push(array.slice(i, i + chunkSize));
  }
  return results;
}

// *** NEW/MODIFIED HEURISTIC EMPLOYEE SELECTOR ***
// This function will attempt to select employees for a SINGLE route
// based on heuristic criteria and a given vehicle capacity, from a pool of *unassigned* employees.
// It returns the selected employees or null/empty array if no suitable batch is found.
async function heuristicallySelectEmployeesForVehicle(
  unassignedEmployeesPool, // Pool of employees not yet routed
  vehicleCapacity,
  facility,
  tripType,
  maxDuration, // Optional max duration for the potential route
  pickupTimePerEmployee,
  guard, // Pass guard flag for heuristic consideration
  profile // Pass profile for constraints like special needs capacity, night shift
) {
  if (!unassignedEmployeesPool || unassignedEmployeesPool.length === 0 || vehicleCapacity === 0) {
    return null; // No employees to select or no capacity
  }

  // Filter employees who don't have valid geo data - they can't be routed anyway
  const availableCandidates = unassignedEmployeesPool.filter(
      (emp) => emp.location && typeof emp.location.lat === 'number' && typeof emp.location.lng === 'number' && !isNaN(emp.location.lat) && !isNaN(emp.location.lng)
  );

   if (availableCandidates.length === 0) {
       return null; // No candidates with valid locations
   }

  const isDropoff = tripType.toLowerCase() === "dropoff";
  const facilityCoordinates = [facility.geoY, facility.geoX];
  const isSpecialNeedsUser = (emp) => (emp.isMedical || false) || (emp.isPWD || false);
  const city  = profile?.name || "Unknown City";

  // Sort employees based on distance to facility for initial selection priority
  // Dropoff: closest first, Pickup: furthest first
  const sortedCandidates = availableCandidates.map((emp) => ({
    ...emp,
    distToFacility: haversineDistance(
      [emp.location.lat, emp.location.lng],
      [facility.geoY, facility.geoX]
    ),
    // Ensure these flags are correctly interpreted later
    isMedical: emp.isMedical || false,
    isPWD: emp.isPWD || false,
  })).sort((a, b) =>
    isDropoff
      ? a.distToFacility - b.distToFacility
      : b.distToFacility - a.distToFacility
  );


  const selectedEmployees = [];
  const MAX_NEXT_STOP_DISTANCE_KM = 1.5 * 1.5; // Reuse swap distance heuristic
  const SCORE_DIFFERENCE_TOLERANCE = 0.1; // Tolerance for score comparison
  const DISTANCE_SCORE_SCALAR = 20; // Scale factor for distance in scoring
  const PROGRESS_WEIGHT = 0.5; // Weight for progress towards/away from facility
  const DISTANCE_WEIGHT = 1.2; // Weight for proximity to the last stop


  let currentRouteMaxAllowedOccupancy = vehicleCapacity;
  let routeIsCurrentlySpecialNeeds = false;


  // *** Start with the best candidate based on the primary sort ***
  const firstEmployeeCandidate = sortedCandidates[0];

   // Check if the first employee fits the capacity and potential special needs constraints
   if (isSpecialNeedsUser(firstEmployeeCandidate)) {
       // If the first person is special needs, this route becomes special needs
       currentRouteMaxAllowedOccupancy = Math.min(vehicleCapacity, 2);
       routeIsCurrentlySpecialNeeds = true;
   }

   // If the capacity of this vehicle is 1, just take the first employee if they fit constraints
   if (vehicleCapacity === 1 && currentRouteMaxAllowedOccupancy >= 1) {
         // Simple case: take the single best employee if valid
         // Check OSRM and duration for the singleton route
          const firstEmpCoords = [firstEmployeeCandidate.location.lat, firstEmployeeCandidate.location.lng];
          const firstRouteCoords = isDropoff
            ? [facilityCoordinates, firstEmpCoords]
            : [firstEmpCoords, facilityCoordinates];

          const firstRouteDetails = await calculateRouteDetails(
            firstRouteCoords,
            [firstEmployeeCandidate],
            pickupTimePerEmployee,
            tripType,
            city
          );

          if (!firstRouteDetails.error && firstRouteDetails.totalDuration <= maxDuration) {
              return [firstEmployeeCandidate]; // Successfully formed a singleton route
          } else {
              console.warn(`Heuristic: Singleton route failed OSRM/duration check for employee ${firstEmployeeCandidate.empCode}. Cannot form batch.`);
              return null; // Cannot route this employee as a singleton with this vehicle
          }
   }


  // *** For capacity > 1, greedily build the route ***
  // Check OSRM and duration for the first employee as a potential route start
   const firstEmpCoords = [firstEmployeeCandidate.location.lat, firstEmployeeCandidate.location.lng];
   const firstRouteCoords = isDropoff
     ? [facilityCoordinates, firstEmpCoords]
     : [firstEmpCoords, facilityCoordinates];

   const firstRouteDetails = await calculateRouteDetails(
     firstRouteCoords,
     [firstEmployeeCandidate],
     pickupTimePerEmployee,
     tripType,
     city
   );

   if (firstRouteDetails.error || firstRouteDetails.totalDuration > maxDuration) {
        console.warn(`Heuristic: First employee ${firstEmployeeCandidate.empCode} failed OSRM/duration check for vehicle capacity ${vehicleCapacity} as route start. Cannot form batch.`);
        return null; // The best starting employee cannot form a valid route even alone
   }

   // Add the first employee as the route start
   selectedEmployees.push(firstEmployeeCandidate);
   const selectedEmployeeCodes = new Set([firstEmployeeCandidate.empCode]);

   // Now iteratively add the best remaining candidates
   let remainingCandidatesForBatch = sortedCandidates.slice(1);

   while (selectedEmployees.length < currentRouteMaxAllowedOccupancy && remainingCandidatesForBatch.length > 0) {
        const currentLastEmployeeInRoute = selectedEmployees[selectedEmployees.length - 1];
        const currentLoc = currentLastEmployeeInRoute.location;

        let scoredCandidates = remainingCandidatesForBatch
          .map((candidateEmp, candidateIdx) => {
            const candidateIsSpecial = isSpecialNeedsUser(candidateEmp);

            // Respect special needs routing segregation - don't add if it violates rules
            if (routeIsCurrentlySpecialNeeds) {
              if (!candidateIsSpecial) return null; // Only add other special needs
            } else {
              if (candidateIsSpecial) {
                 // If route is NOT special needs yet, but candidate IS, check if it's allowed to become one
                 if (!isSpecialNeedsUser(selectedEmployees[0])) {
                      return null; // Cannot mix regular and special needs if the first person was regular
                 }
              }
            }

            const distanceToLastHaversine = haversineDistance(
              [currentLoc.lat, currentLoc.lng],
              [candidateEmp.location.lat, candidateEmp.location.lng]
            );
            if (distanceToLastHaversine > MAX_NEXT_STOP_DISTANCE_KM) return null;

            // Simple score: prioritize closer distance and progress towards/away from facility
            let progressMetric = isDropoff
                ? candidateEmp.distToFacility - currentLastEmployeeInRoute.distToFacility
                : currentLastEmployeeInRoute.distToFacility - candidateEmp.distToFacility;

            const score = (progressMetric * PROGRESS_WEIGHT) + ((MAX_NEXT_STOP_DISTANCE_KM - distanceToLastHaversine) * (DISTANCE_WEIGHT * DISTANCE_SCORE_SCALAR / MAX_NEXT_STOP_DISTANCE_KM)); // Normalize distance score


            return {
              emp: candidateEmp,
              score: score,
              distanceToLast: distanceToLastHaversine,
              originalIndex: candidateIdx,
            };
          })
          .filter((item) => item != null); // Remove null entries (those that failed initial checks)

        // Sort candidates primarily by score (higher is better), secondarily by distance (lower is better)
        scoredCandidates.sort((a, b) => {
          if (Math.abs(b.score - a.score) > SCORE_DIFFERENCE_TOLERANCE)
            return b.score - a.score;
          return a.distanceToLast - b.distanceToLast;
        });


        if (scoredCandidates.length === 0) {
             // No more viable candidates can be added to this route
             break;
        }

        // Try adding the top candidate and validate the resulting route with OSRM/duration
        const nextEmployeeToPickData = scoredCandidates[0];
        const nextEmployeeToPick = nextEmployeeToPickData?.emp;

         if (!nextEmployeeToPick) { // Should not happen if scoredCandidates.length > 0, but safety check
             break;
         }

        const tentativeEmployees = [...selectedEmployees, nextEmployeeToPick];
         const tentativeCoords = tentativeEmployees.map(emp => [emp.location.lat, emp.location.lng]);
         const allCoords = isDropoff
           ? [facilityCoordinates, ...tentativeCoords]
           : [...tentativeCoords, facilityCoordinates];

         const tentativeDetails = await calculateRouteDetails(
           allCoords,
           tentativeEmployees,
           pickupTimePerEmployee,
           tripType,
           city
         );

         if (!tentativeDetails.error && tentativeDetails.totalDuration <= maxDuration) {
             // Candidate passed OSRM and duration check, add to the route
             selectedEmployees.push(nextEmployeeToPick);
             selectedEmployeeCodes.add(nextEmployeeToPick.empCode);

             // Update route special needs status and capacity if this addition makes it so
             if (isSpecialNeedsUser(nextEmployeeToPick) && !routeIsCurrentlySpecialNeeds) {
                  routeIsCurrentlySpecialNeeds = true;
                  currentRouteMaxAllowedOccupancy = Math.min(vehicleCapacity, 2);
             }

             // Remove the added employee from the remaining candidates pool
             const actualPickedIndexInCandidates = remainingCandidatesForBatch.findIndex(
                 (e) => e.empCode === nextEmployeeToPick.empCode
             );
             if (actualPickedIndexInCandidates > -1) {
                  remainingCandidatesForBatch.splice(actualPickedIndexInCandidates, 1);
             }
              // Note: remainingCandidatesForBatch is modified in place, affecting subsequent loop iterations
         } else {
             // Candidate didn't fit (OSRM failed or duration exceeded), remove from consideration *for this vehicle run*
              console.warn(`Heuristic: Candidate ${nextEmployeeToPick.empCode} failed OSRM/duration check when added to route for vehicle capacity ${vehicleCapacity}. Skipping this candidate for this batch.`);
             const actualPickedIndexInCandidates = remainingCandidatesForBatch.findIndex(
                 (e) => e.empCode === nextEmployeeToPick.empCode
             );
             if (actualPickedIndexInCandidates > -1) {
                  remainingCandidatesForBatch.splice(actualPickedIndexInCandidates, 1);
             }
         }
   }

   // After attempting to fill the vehicle, if we have selected employees, return the batch
   if (selectedEmployees.length > 0) {
       // Optionally, check guard requirement here before returning
       let routeNeedsGuard = false;
       if (guard && selectedEmployees.length > 0 && isNightShiftForGuard(facility.shiftTime, tripType, profile)) {
            const critIdx = isDropoff ? selectedEmployees.length - 1 : 0;
            if (selectedEmployees[critIdx].gender === "F" && !selectedEmployees.some((e) => e.gender === "M")) {
                routeNeedsGuard = true;
            }
       }
        return selectedEmployees.map(emp => ({...emp, guardNeeded: routeNeedsGuard})); // Attach guard info to employees for this batch
   } else {
       // Could not form a valid batch even with the first employee
       return null;
   }
}

module.exports = {
  generateRoutes,
  isOsrmAvailable,
};