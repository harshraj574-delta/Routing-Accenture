const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const { spawn } = require("child_process");


// const TRAFFIC_BUFFER_PERCENTAGE = 0.4; // 40% buffer for traffic
const MAX_SWAP_DISTANCE_KM = 1.5;

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
    return 0.6; // 60% buffer
  } else if (decimalTime >= 10.0 && decimalTime < 16.0) {
    // Afternoon: 10:00 AM - 4:00 PM (Moderate traffic)
    return 0.6; // 30% buffer
  } else if (decimalTime >= 16.0 && decimalTime < 20.0) {
    // Evening rush hour: 4:00 PM - 8:00 PM (High traffic)
    return 0.6; // 60% buffer
  } else {
    // Night time: 8:00 PM - 7:00 AM (Low traffic)
    return 0.6; // 20% buffer
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
    console.log("this is it ", error);
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

// ==================== ANGULAR SECTOR CLUSTERING FOR LINEAR ROUTES ====================
/**
 * Calculate bearing (direction) from point A to point B in degrees (0-360)
 * 0° = North, 90° = East, 180° = South, 270° = West
 */
function calculateBearing(fromLat, fromLng, toLat, toLng) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const toDeg = (rad) => (rad * 180) / Math.PI;

  const dLng = toRad(toLng - fromLng);
  const lat1 = toRad(fromLat);
  const lat2 = toRad(toLat);

  const y = Math.sin(dLng) * Math.cos(lat2);
  const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLng);

  let bearing = toDeg(Math.atan2(y, x));
  return (bearing + 360) % 360; // Normalize to 0-360
}

/**
 * Assign employees to angular sectors based on their direction from facility
 * This ensures employees in the same direction are grouped together for linear routes
 * @param {Array} employees - List of employees with location data
 * @param {Object} facility - Facility with geoY (lat) and geoX (lng)
 * @param {number} numSectors - Number of sectors to divide into (default 8 = 45° each)
 * @returns {Map} Map of sectorIndex -> [employees in that sector]
 */
function assignAngularSectors(employees, facility, numSectors = 8) {
  const sectorSize = 360 / numSectors;
  const sectors = new Map();

  for (let i = 0; i < numSectors; i++) {
    sectors.set(i, []);
  }

  for (const emp of employees) {
    if (!emp.location?.lat || !emp.location?.lng) continue;

    const bearing = calculateBearing(
      facility.geoY, facility.geoX,
      emp.location.lat, emp.location.lng
    );

    const sectorIndex = Math.floor(bearing / sectorSize) % numSectors;
    const empWithBearing = {
      ...emp,
      bearingFromFacility: bearing,
      distToFacility: haversineDistance(
        [emp.location.lat, emp.location.lng],
        [facility.geoY, facility.geoX]
      )
    };
    sectors.get(sectorIndex).push(empWithBearing);
  }

  // Sort each sector by distance to facility (farthest first for pickup, closest first for dropoff)
  sectors.forEach((emps, sectorIdx) => {
    sectors.set(sectorIdx, emps.sort((a, b) => b.distToFacility - a.distToFacility));
  });

  return sectors;
}

/**
 * Check if candidate employee maintains linear direction
 * For PICKUP: checks if candidate is "on the way" toward facility
 * For DROP: checks if candidate is "on the way" away from facility (continuing outward)
 */
function isLinearDirection(currentEmp, candidateEmp, facility, toleranceDegrees = 60, tripType = 'pickup') {
  const isDropoff = tripType.toLowerCase() === 'dropoff';

  if (isDropoff) {
    // For DROP: we're moving AWAY from facility, so check if candidate continues that direction
    // Calculate bearing from facility to current employee (the "outward" direction)
    const bearingOutward = calculateBearing(
      facility.geoY, facility.geoX,
      currentEmp.location.lat, currentEmp.location.lng
    );

    // Calculate bearing from current to candidate
    const bearingToCandidate = calculateBearing(
      currentEmp.location.lat, currentEmp.location.lng,
      candidateEmp.location.lat, candidateEmp.location.lng
    );

    // Calculate angular difference - candidate should continue the outward direction
    let diff = Math.abs(bearingOutward - bearingToCandidate);
    if (diff > 180) diff = 360 - diff;

    return diff <= toleranceDegrees;
  } else {
    // For PICKUP: we're moving TOWARD facility
    // Calculate bearing from current to facility
    const bearingToFacility = calculateBearing(
      currentEmp.location.lat, currentEmp.location.lng,
      facility.geoY, facility.geoX
    );

    // Calculate bearing from current to candidate
    const bearingToCandidate = calculateBearing(
      currentEmp.location.lat, currentEmp.location.lng,
      candidateEmp.location.lat, candidateEmp.location.lng
    );

    // Calculate angular difference
    let diff = Math.abs(bearingToFacility - bearingToCandidate);
    if (diff > 180) diff = 360 - diff;

    // If candidate is within tolerance of the direction to facility, it's linear
    return diff <= toleranceDegrees;
  }
}

// ==================== SINGLETON AGGREGATION ====================
/**
 * Aggregate singleton routes into larger routes where possible
 * @param {Array} routes - List of routes, some may be singletons
 * @param {Object} facility - Facility location
 * @param {string} tripType - 'pickup' or 'dropoff'
 * @param {number} maxClusterRadius - Maximum distance to cluster singletons together
 * @param {number} maxClusterSize - Maximum employees per aggregated route
 * @returns {Array} - Routes with singletons aggregated where possible
 */
function aggregateSingletonRoutes(routes, facility, tripType, maxClusterRadius = 5, maxClusterSize = 4) {
  const singletons = routes.filter(r => r.employees && r.employees.length === 1);
  const nonSingletons = routes.filter(r => !r.employees || r.employees.length !== 1);

  if (singletons.length < 2) {
    return routes; // Nothing to aggregate
  }

  console.log(`\n[Singleton Aggregation] Found ${singletons.length} singleton routes to process`);

  // Extract employees from singletons with their route info
  const singletonEmployees = singletons.map(route => ({
    employee: route.employees[0],
    originalRoute: route,
    bearing: calculateBearing(
      facility.geoY, facility.geoX,
      route.employees[0].location.lat, route.employees[0].location.lng
    ),
    distToFacility: haversineDistance(
      [route.employees[0].location.lat, route.employees[0].location.lng],
      [facility.geoY, facility.geoX]
    )
  }));

  // Sort by bearing to group employees in similar directions
  singletonEmployees.sort((a, b) => a.bearing - b.bearing);

  // Cluster using simple greedy approach based on angular proximity and distance
  const clusters = [];
  const used = new Set();

  for (let i = 0; i < singletonEmployees.length; i++) {
    if (used.has(i)) continue;

    const cluster = [singletonEmployees[i]];
    used.add(i);

    // Find nearby singletons in similar direction
    for (let j = i + 1; j < singletonEmployees.length && cluster.length < maxClusterSize; j++) {
      if (used.has(j)) continue;

      const candidate = singletonEmployees[j];
      const lastInCluster = cluster[cluster.length - 1];

      // Check angular proximity (within 45 degrees)
      let bearingDiff = Math.abs(candidate.bearing - lastInCluster.bearing);
      if (bearingDiff > 180) bearingDiff = 360 - bearingDiff;

      if (bearingDiff > 45) continue;

      // Check distance proximity
      const distBetween = haversineDistance(
        [lastInCluster.employee.location.lat, lastInCluster.employee.location.lng],
        [candidate.employee.location.lat, candidate.employee.location.lng]
      );

      if (distBetween <= maxClusterRadius) {
        cluster.push(candidate);
        used.add(j);
      }
    }

    if (cluster.length > 1) {
      clusters.push(cluster);
    } else {
      // Keep as singleton
      clusters.push(cluster);
    }
  }

  // Convert clusters back to routes
  const aggregatedRoutes = [];
  let aggregatedCount = 0;

  for (const cluster of clusters) {
    if (cluster.length === 1) {
      // Keep original singleton route
      aggregatedRoutes.push(cluster[0].originalRoute);
    } else {
      // Create new aggregated route from the first singleton's route structure
      const baseRoute = cluster[0].originalRoute;

      // Sort cluster employees by distance for proper ordering
      const isDropoff = tripType.toLowerCase() === 'dropoff';
      const sortedEmployees = cluster
        .map(c => c.employee)
        .sort((a, b) => isDropoff
          ? haversineDistance([a.location.lat, a.location.lng], [facility.geoY, facility.geoX]) -
          haversineDistance([b.location.lat, b.location.lng], [facility.geoY, facility.geoX])
          : haversineDistance([b.location.lat, b.location.lng], [facility.geoY, facility.geoX]) -
          haversineDistance([a.location.lat, a.location.lng], [facility.geoY, facility.geoX])
        )
        .map((emp, idx) => ({ ...emp, order: idx + 1 }));

      aggregatedRoutes.push({
        ...baseRoute,
        employees: sortedEmployees,
        uniqueKey: `AGGREGATED_${uuidv4()}`,
        aggregatedSingletons: cluster.map(c => c.originalRoute.uniqueKey),
        // Force recalculation of route geometry for merged singleton routes.
        // Without this, the aggregated route can retain base singleton polyline.
        encodedPolyline: "",
        routeDetails: null
      });

      aggregatedCount += cluster.length;
      console.log(`[Singleton Aggregation] Merged ${cluster.length} singletons: ${sortedEmployees.map(e => e.empCode).join(', ')}`);
    }
  }

  console.log(`[Singleton Aggregation] Complete. Aggregated ${aggregatedCount} singletons into ${clusters.filter(c => c.length > 1).length} routes`);

  return [...nonSingletons, ...aggregatedRoutes];
}
// ==================== END ANGULAR SECTOR CLUSTERING ====================

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
    // console.log(`[RouteDeviation] BYPASSING deviation check for route ${route.uniqueKey || route.routeNumber} (global override).`);
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
      `[checkRouteDeviation] Route ${route.uniqueKey || route.routeNumber
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
      `[checkRouteDeviation] Could not determine applicable rule for Route ${route.uniqueKey || route.routeNumber
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
      `[checkRouteDeviation] Route ${route.uniqueKey || route.routeNumber
      }: DEVIATION EXCEEDED. Rule: ${applicableRule.minDistKm}-${applicableRule.maxDistKm
      }km (maxTotal: ${applicableRule.maxTotalOneWayKm
      }km). FarthestEmpDist: ${farthestEmployeeDistanceKm.toFixed(
        3
      )}km. ActualRouteDist: ${relevantRouteDistanceKm.toFixed(
        3
      )}km. Returning false.`
    );
    return false;
  }

  // console.log(
  //   `[checkRouteDeviation] Route ${
  //     route.uniqueKey || route.routeNumber
  //   }: PASSED. Rule: ${applicableRule.minDistKm}-${
  //     applicableRule.maxDistKm
  //   }km (maxTotal: ${
  //     applicableRule.maxTotalOneWayKm
  //   }km). FarthestEmpDist: ${farthestEmployeeDistanceKm.toFixed(
  //     3
  //   )}km. ActualRouteDist: ${relevantRouteDistanceKm.toFixed(3)}km.`
  // );
  return true;
}

async function calculateEta(currentGeocodes, destinationGeocodes, shiftTime, city) {
  const fastApiCity = getFastApiCityKey(city);
  const trafficBuffer = getTrafficBufferForShiftTime(shiftTime);

  const coordinates = [
    [currentGeocodes.lng, currentGeocodes.lat],
    [destinationGeocodes.lng, destinationGeocodes.lat]
  ];

  try {
    const response = await fetchApi(`https://mapapi.etmsonline.in/table`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        city: fastApiCity,
        coordinates: coordinates,
        annotations: 'duration'
      })
    });

    if (!response.ok) {
      throw new Error(`OSRM API responded with status ${response.status}`);
    }

    const data = await response.json();

    if (data.code === 'Ok' && data.durations && data.durations.length > 1) {
      const baseDuration = data.durations[0][1];
      return Math.round(baseDuration * (1 + trafficBuffer));
    } else {
      throw new Error('Invalid response from OSRM API');
    }
  } catch (error) {
    console.error('Error in calculateEta:', error);
    return -1; // Return -1 or handle the error as you see fit
  }
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
  let TRAFFIC_BUFFER_PERCENTAGE = getTrafficBufferForShiftTime(shiftTime);
  TRAFFIC_BUFFER_PERCENTAGE = Math.min(TRAFFIC_BUFFER_PERCENTAGE, 0.8); // ← Same cap as calculatePickupTimes
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
      // console.log(
      //   "[DEBUG calculateRouteDetails] routeObject.distance:",
      //   data.routes[0].distance
      // );
    }
    if (data.code !== "Ok" || !data.routes || data.routes.length === 0) {
      console.error(
        "[OSRM /route] API returned non-Ok code or no routes:",
        data,
        ` for URL: ${url}`
      );
      throw new Error(
        `Invalid OSRM /route response: ${data.code || "Unknown code"
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
  // Prepare coordinates as [lng, lat] pairs for OSRM (OSRM uses [lng, lat] format!)
  const coords = allPointsCoords.map((p) => [p.lng, p.lat]);

  console.log(`[DEBUG-OSRM-REQ] City: ${fastApiCity}, Coords count: ${coords.length}`);
  console.log(`[DEBUG-OSRM-REQ] First 3 coords: ${JSON.stringify(coords.slice(0, 3))}`);

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
      `OSRM table service error for matrix: ${response.status
      }. Body: ${errorText.substring(0, 200)}`
    );
  }
  const data = await response.json();
  if (data.code !== "Ok" || !data.durations || !data.distances) {
    throw new Error(
      "Invalid OSRM table response for matrix (structure or code)"
    );
  }

  console.log(`[DEBUG-OSRM] Response code: ${data.code}, distances[0][1]=${data.distances?.[0]?.[1]}, durations[0][1]=${data.durations?.[0]?.[1]}`);

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

// Helper function to get NMT capacity limit
function getNMTCapacityLimit(fleetConfig) {
  const smallVehicle = fleetConfig.find(vehicle => vehicle.type === "s");
  return smallVehicle ? smallVehicle.capacity : 3; // fallback to 3 if no 's' defined
}

/**
 * Analyze fleet configuration and determine optimal routing strategy
 */
function analyzeFleetConfiguration(fleet) {
  if (!fleet || !Array.isArray(fleet) || fleet.length === 0) {
    return {
      vehicleTypes: [],
      capacityRanges: {},
      optimalSizes: {},
      totalCapacity: 0
    };
  }

  const sortedFleet = [...fleet].sort((a, b) => a.capacity - b.capacity);

  const analysis = {
    vehicleTypes: sortedFleet,
    capacityRanges: {},
    optimalSizes: {},
    totalCapacity: fleet.reduce((sum, v) => sum + (v.capacity * v.count), 0),
    fleetDistribution: {}
  };

  sortedFleet.forEach((vehicle, index) => {
    const type = vehicle.type;
    const capacity = vehicle.capacity;

    let minEmployees, maxEmployees;

    if (index === 0) {
      minEmployees = 1;
      maxEmployees = capacity;
    } else {
      const prevCapacity = sortedFleet[index - 1].capacity;
      minEmployees = prevCapacity + 1;
      maxEmployees = capacity;
    }

    analysis.capacityRanges[type] = {
      minEmployees,
      maxEmployees,
      capacity,
      count: vehicle.count,
      utilization: 0
    };

    // **NEW: Target MAXIMUM capacity, not capacity - 1**
    analysis.optimalSizes[type] = capacity; // Full capacity utilization

    analysis.fleetDistribution[type] = vehicle.count / fleet.reduce((sum, v) => sum + v.count, 0);
  });

  console.log('[Fleet Analysis] MAXIMUM Capacity Configuration:', JSON.stringify(analysis.capacityRanges, null, 2));
  console.log('[Fleet Analysis] MAXIMUM Optimal sizes:', analysis.optimalSizes);

  return analysis;
}

/**
 * Get recommended route size based on fleet configuration and current availability
 */
function getRecommendedRouteSize(fleetAnalysis, availableFleetCounts, zoneName) {
  if (!fleetAnalysis.vehicleTypes || fleetAnalysis.vehicleTypes.length === 0) {
    return 6; // fallback
  }

  // Find the vehicle type with the best availability vs utilization ratio
  let bestType = null;
  let bestScore = -1;

  fleetAnalysis.vehicleTypes.forEach(vehicle => {
    const type = vehicle.type;
    const available = availableFleetCounts[type] || 0;
    const total = vehicle.count;
    const utilizationRate = (total - available) / total;
    const availabilityRate = available / total;

    // Prefer underutilized types with good availability
    const score = availabilityRate * (1 - utilizationRate) * fleetAnalysis.fleetDistribution[type];

    if (score > bestScore && available > 0) {
      bestScore = score;
      bestType = type;
    }
  });

  if (bestType) {
    const recommendedSize = fleetAnalysis.optimalSizes[bestType];
    console.log(`[Route Size] Zone ${zoneName}: Targeting ${bestType} type (capacity: ${fleetAnalysis.capacityRanges[bestType].capacity}) → recommended size: ${recommendedSize}`);
    return recommendedSize;
  }

  // Fallback: use the most available vehicle type
  const fallbackType = fleetAnalysis.vehicleTypes.find(v => (availableFleetCounts[v.type] || 0) > 0);
  return fallbackType ? fleetAnalysis.optimalSizes[fallbackType.type] : 4;
}

async function assignVehicleAndFinalizeGroup(
  routeShell,
  preliminaryEmployeesInGroup,
  profile,
  availableFleetCounts,
  shiftTime,
  tripType,
  activateGuardSystem,
  fleetAnalysis
) {
  let currentEmployeesForRoute = [...preliminaryEmployeesInGroup];
  let employeesTrimmedOff = [];

  routeShell.employees = [];
  routeShell.error = false;
  routeShell.errorMessage = "";
  routeShell.afterFleetExhaustion = false;
  routeShell.assignedVehicleType = "NONE";
  routeShell.vehicleCapacity = 0;
  let preliminaryGuardNeeded = false;

  if (!currentEmployeesForRoute || currentEmployeesForRoute.length === 0) {
    routeShell.guardNeeded = false;
    return { employeesTrimmedOff };
  }

  const isDropoff = tripType.toLowerCase() === "dropoff";

  // **CONSTRAINT 1: NMT Check and Early Trimming**
  const hasNMTEmployee = currentEmployeesForRoute.some(emp => emp.isNMT === true);
  const nmtCapacityLimit = hasNMTEmployee ? getNMTCapacityLimit(profile.fleet || []) : null;

  if (hasNMTEmployee) {
    console.log(`[NMT Route EARLY] Route ${routeShell.uniqueKey} contains NMT employee(s). Must limit to ${nmtCapacityLimit} employees TOTAL.`);
    routeShell.isNMTRoute = true;
    routeShell.nmtCapacityLimit = nmtCapacityLimit;

    // **IMMEDIATE TRIMMING: Reduce to NMT limit BEFORE vehicle assignment**
    if (currentEmployeesForRoute.length > nmtCapacityLimit) {
      const originalCount = currentEmployeesForRoute.length;

      // Keep the first nmtCapacityLimit employees, trim the rest
      const employeesToKeep = currentEmployeesForRoute.slice(0, nmtCapacityLimit);
      const employeesToTrim = currentEmployeesForRoute.slice(nmtCapacityLimit);

      currentEmployeesForRoute = employeesToKeep;
      employeesTrimmedOff.push(...employeesToTrim);

      console.log(`[NMT EARLY TRIM] Route ${routeShell.uniqueKey}: Trimmed ${employeesToTrim.length} employees (${originalCount} → ${currentEmployeesForRoute.length}) due to NMT capacity limit`);

      // Log which employees were trimmed
      employeesToTrim.forEach(emp => {
        console.log(`[NMT Early Trimming] Employee ${emp.empCode} trimmed from NMT route ${routeShell.uniqueKey} due to NMT capacity limit (${nmtCapacityLimit})`);
      });
    }
  }

  // **CONSTRAINT 2: Guard Logic (check AFTER NMT trimming)**
  if (activateGuardSystem && currentEmployeesForRoute.length > 0) {
    const critIdx = isDropoff ? currentEmployeesForRoute.length - 1 : 0;
    if (currentEmployeesForRoute[critIdx]?.gender === "F") {
      preliminaryGuardNeeded = true;
      console.log(`[Guard Needed] Route ${routeShell.uniqueKey}: Guard required for female employee at critical position`);
    }
  }
  routeShell.guardNeeded = preliminaryGuardNeeded;

  let requiredVehicleOccupancy = currentEmployeesForRoute.length + (preliminaryGuardNeeded ? 1 : 0);

  // **For NMT routes, the vehicle selection occupancy is already correct after early trimming**
  let vehicleSelectionOccupancy = requiredVehicleOccupancy;
  if (hasNMTEmployee) {
    console.log(`[NMT Vehicle Selection] Route ${routeShell.uniqueKey} final occupancy: ${vehicleSelectionOccupancy} (${currentEmployeesForRoute.length} employees + ${preliminaryGuardNeeded ? 1 : 0} guard)`);
  }

  const sortedFleet = [...(profile.fleet || [])].sort((a, b) => a.capacity - b.capacity);
  let assignedVehicleConfig = null;

  // **Vehicle Selection**
  if (sortedFleet.length > 0) {
    assignedVehicleConfig = selectBestFitVehicle(
      sortedFleet,
      vehicleSelectionOccupancy,
      availableFleetCounts,
      profile,
      fleetAnalysis
    );
  }

  if (assignedVehicleConfig) {
    availableFleetCounts[assignedVehicleConfig.type]--;
    routeShell.afterFleetExhaustion = false;

    if (assignedVehicleConfig.type === 'm') {
      console.log(`[Medium Vehicle] ✅ Successfully assigned medium vehicle to route ${routeShell.uniqueKey} (occupancy: ${vehicleSelectionOccupancy})`);
    }
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
      console.log(`[Fleet Exhaustion] Route ${routeShell.uniqueKey} using fallback medium vehicle (all suitable vehicles exhausted)`);
      if (assignedVehicleConfig.capacity < requiredVehicleOccupancy) {
        console.warn(`[Fleet] Route ${routeShell.uniqueKey} (Req Occupancy: ${requiredVehicleOccupancy}) assigned fallback 'm' (Cap: ${assignedVehicleConfig.capacity}) which is smaller. Trimming will occur.`);
      }
    }
  }

  routeShell.assignedVehicleType = assignedVehicleConfig.type;
  routeShell.vehicleCapacity = assignedVehicleConfig.capacity;

  if (routeShell.error) {
    routeShell.employees = [];
    return { employeesTrimmedOff };
  }

  // NMT logging for effective capacity
  if (hasNMTEmployee) {
    console.log(`[NMT Effective Capacity] Route ${routeShell.uniqueKey} vehicle capacity: ${routeShell.vehicleCapacity}, NMT limited to: ${nmtCapacityLimit}`);
  }

  // --- START: CORRECTED FINAL TRIMMING LOGIC ---
  let finalIsSpecial;
  let trimmingIteration = 0;
  const MAX_TRIMMING_ITERATIONS = currentEmployeesForRoute.length + 3;

  while (
    trimmingIteration++ < MAX_TRIMMING_ITERATIONS &&
    currentEmployeesForRoute.length > 0
  ) {
    let groupIsOverCapacity = false;
    let reason = "";

    // Check 1: Total occupants vs. Vehicle Capacity
    const totalOccupants = currentEmployeesForRoute.length + (routeShell.guardNeeded ? 1 : 0);
    if (totalOccupants > routeShell.vehicleCapacity) {
      groupIsOverCapacity = true;
      reason = `Occupants (${totalOccupants}) > Vehicle Capacity (${routeShell.vehicleCapacity})`;
    }

    // Check 2: Special Needs Employee Constraints
    finalIsSpecial = currentEmployeesForRoute.some(isSpecialNeedsUser);
    if (finalIsSpecial) {
      const specialNeedsMax = routeShell.guardNeeded ? 1 : 2;
      if (currentEmployeesForRoute.length > specialNeedsMax) {
        groupIsOverCapacity = true;
        reason = `Special Needs employee count (${currentEmployeesForRoute.length}) > limit (${specialNeedsMax})`;
        console.log(`[Special Needs Constraint] Route ${routeShell.uniqueKey}: Max passengers reduced to ${specialNeedsMax}`);
      }
    }

    // Check 3: NMT Employee Count Constraint (this is a safeguard, should be handled by early trimming)
    if (hasNMTEmployee && currentEmployeesForRoute.length > nmtCapacityLimit) {
      groupIsOverCapacity = true;
      reason = `NMT employee count (${currentEmployeesForRoute.length}) > limit (${nmtCapacityLimit})`;
      console.log(`[NMT Final Check] ${reason}.`);
    }

    if (groupIsOverCapacity) {
      const empToTrim = isDropoff ? currentEmployeesForRoute.shift() : currentEmployeesForRoute.pop();

      if (empToTrim) {
        employeesTrimmedOff.push(empToTrim);
        console.log(`[Final Trimming] Employee ${empToTrim.empCode} trimmed from route ${routeShell.uniqueKey} due to capacity constraints (Special: ${finalIsSpecial}, Guard: ${routeShell.guardNeeded})`);
      }

      // Re-check guard needed after trimming as the critical employee might have changed
      if (activateGuardSystem && currentEmployeesForRoute.length > 0) {
        const critIdxRecheck = isDropoff ? currentEmployeesForRoute.length - 1 : 0;
        const newGuardNeededStatus = currentEmployeesForRoute[critIdxRecheck]?.gender === "F";
        if (routeShell.guardNeeded !== newGuardNeededStatus) {
          routeShell.guardNeeded = newGuardNeededStatus;
          console.log(`[Guard Status Change] Route ${routeShell.uniqueKey}: Guard needed changed to ${newGuardNeededStatus} after trimming`);
        }
      } else if (currentEmployeesForRoute.length === 0 && routeShell.guardNeeded) {
        routeShell.guardNeeded = false;
      }
    } else {
      // If no capacity constraint is violated, the loop can end.
      break;
    }
  }
  // --- END: CORRECTED FINAL TRIMMING LOGIC ---

  if (trimmingIteration >= MAX_TRIMMING_ITERATIONS) {
    console.warn(`[Fleet Trim] Max iterations reached for ${routeShell.uniqueKey}`);
  }

  routeShell.employees = [...currentEmployeesForRoute];
  routeShell.isSpecialNeedsRoute = routeShell.employees.some(isSpecialNeedsUser);

  if (routeShell.employees.length === 0 && preliminaryEmployeesInGroup.length > 0 && !routeShell.error) {
    routeShell.error = true;
    routeShell.errorMessage = `Route became empty after vehicle assignment and trimming (Vehicle: ${routeShell.assignedVehicleType})`;
  }

  if (employeesTrimmedOff.length > 0) {
    console.log(`[Fleet] Route ${routeShell.uniqueKey} (Type: ${routeShell.assignedVehicleType}, Final Emps: ${routeShell.employees.length}) trimmed ${employeesTrimmedOff.length} employees.`);
  }

  // **VALIDATION: Final checks for all constraints**
  if (hasNMTEmployee && routeShell.employees.length > nmtCapacityLimit) {
    console.error(`[NMT VALIDATION ERROR] Route ${routeShell.uniqueKey} still has ${routeShell.employees.length} employees but NMT limit is ${nmtCapacityLimit}!`);
  }

  if (routeShell.isSpecialNeedsRoute && routeShell.employees.length > (routeShell.guardNeeded ? 1 : 2)) {
    console.error(`[Special Needs VALIDATION ERROR] Route ${routeShell.uniqueKey} has ${routeShell.employees.length} special needs employees but limit is ${routeShell.guardNeeded ? 1 : 2}!`);
  }

  return { employeesTrimmedOff };
}

/**
 * Select the best-fit vehicle for optimal fleet utilization
 * @param {Array} sortedFleet - Fleet sorted by capacity
 * @param {number} vehicleSelectionOccupancy - Required occupancy
 * @param {Object} availableFleetCounts - Current available fleet counts
 * @param {Object} profile - Profile containing original fleet configuration
 * @returns {Object|null} Selected vehicle configuration or null
 */

/**
 * Enhanced vehicle selection that works with any fleet configuration
 */
function selectBestFitVehicle(sortedFleet, vehicleSelectionOccupancy, availableFleetCounts, profile, fleetAnalysis) {
  const suitableVehicles = sortedFleet.filter(vehicle =>
    vehicle.capacity >= vehicleSelectionOccupancy &&
    availableFleetCounts[vehicle.type] > 0
  );

  if (suitableVehicles.length === 0) {
    console.warn(`[Fleet Selection] No suitable vehicles available for occupancy ${vehicleSelectionOccupancy}`);
    return null;
  }

  // **NEW: Perfect capacity match (high utilization) gets highest priority**
  const highUtilizationMatches = suitableVehicles.filter(vehicle => {
    const utilizationRate = vehicleSelectionOccupancy / vehicle.capacity;
    return utilizationRate >= 0.75; // 75% or higher utilization
  });

  if (highUtilizationMatches.length > 0) {
    // Among high utilization matches, prefer the one with highest utilization
    const bestUtilization = highUtilizationMatches.sort((a, b) => {
      const utilizationA = vehicleSelectionOccupancy / a.capacity;
      const utilizationB = vehicleSelectionOccupancy / b.capacity;
      return utilizationB - utilizationA;
    })[0];

    console.log(`[Fleet High Utilization] Occupancy ${vehicleSelectionOccupancy} → HIGH UTILIZATION: ${bestUtilization.type}(${bestUtilization.capacity}) - ${(vehicleSelectionOccupancy / bestUtilization.capacity * 100).toFixed(1)}% utilized`);
    return bestUtilization;
  }

  // **NEW: Medium utilization matches**
  const mediumUtilizationMatches = suitableVehicles.filter(vehicle => {
    const utilizationRate = vehicleSelectionOccupancy / vehicle.capacity;
    return utilizationRate >= 0.5 && utilizationRate < 0.75;
  });

  if (mediumUtilizationMatches.length > 0) {
    const bestMedium = mediumUtilizationMatches.sort((a, b) => {
      const utilizationA = vehicleSelectionOccupancy / a.capacity;
      const utilizationB = vehicleSelectionOccupancy / b.capacity;
      return utilizationB - utilizationA;
    })[0];

    console.log(`[Fleet Medium Utilization] Occupancy ${vehicleSelectionOccupancy} → MEDIUM UTILIZATION: ${bestMedium.type}(${bestMedium.capacity}) - ${(vehicleSelectionOccupancy / bestMedium.capacity * 100).toFixed(1)}% utilized`);
    return bestMedium;
  }

  // **Fallback: Enhanced scoring with utilization bonus**
  const vehicleScores = suitableVehicles.map(vehicle => {
    const originalCount = profile.fleet.find(f => f.type === vehicle.type)?.count || 1;
    const usedCount = originalCount - availableFleetCounts[vehicle.type];
    const fleetUtilizationRate = usedCount / originalCount;

    const capacityUtilization = vehicleSelectionOccupancy / vehicle.capacity;
    const capacityWaste = vehicle.capacity - vehicleSelectionOccupancy;
    const capacityEfficiency = 1 / (1 + (capacityWaste / vehicle.capacity));
    const utilizationBalance = Math.max(0, 1 - fleetUtilizationRate);
    const distributionBonus = fleetAnalysis.fleetDistribution[vehicle.type] || 0;

    // **NEW: High capacity utilization bonus**
    let utilizationBonus = 0;
    if (capacityUtilization >= 0.8) {
      utilizationBonus = 0.3; // Strong bonus for high utilization
    } else if (capacityUtilization >= 0.6) {
      utilizationBonus = 0.2; // Medium bonus
    } else if (capacityUtilization >= 0.4) {
      utilizationBonus = 0.1; // Small bonus
    }

    const score = (capacityEfficiency * 0.3) +
      (utilizationBalance * 0.2) +
      (distributionBonus * 0.1) +
      (utilizationBonus * 0.4); // **Strong emphasis on utilization**

    return {
      vehicle,
      score,
      capacityWaste,
      capacityUtilization: capacityUtilization * 100,
      fleetUtilizationRate: fleetUtilizationRate * 100,
      utilizationBonus
    };
  });

  vehicleScores.sort((a, b) => b.score - a.score);
  const selected = vehicleScores[0];

  console.log(`[Fleet Utilization Focus] Occupancy ${vehicleSelectionOccupancy} → Options:`,
    vehicleScores.map(v =>
      `${v.vehicle.type}(cap:${v.vehicle.capacity}, util:${v.capacityUtilization.toFixed(1)}%, score:${v.score.toFixed(3)})`
    ).join(' | '),
    `→ SELECTED: ${selected.vehicle.type} (${selected.capacityUtilization.toFixed(1)}% utilized)`
  );

  return selected.vehicle;
}

/**
 * Adjust target group size based on fleet availability to maximize utilization
 */
/**
 * Dynamic zone capacity based on actual fleet configuration
 */
function getOptimalGroupSizeForZone(zoneName, profile, availableFleetCounts, fleetAnalysis) {
  if (!fleetAnalysis || !fleetAnalysis.vehicleTypes.length) {
    return 6; // fallback
  }

  // **NEW: Calculate total available capacity vs remaining employees**
  const totalAvailableCapacity = fleetAnalysis.vehicleTypes.reduce((sum, vehicle) => {
    const available = availableFleetCounts[vehicle.type] || 0;
    return sum + (available * vehicle.capacity);
  }, 0);

  // **NEW: Prioritize by capacity and availability**
  const fleetByCapacity = fleetAnalysis.vehicleTypes
    .map(vehicle => {
      const available = availableFleetCounts[vehicle.type] || 0;
      const total = vehicle.count;
      const utilizationRate = (total - available) / total;

      return {
        type: vehicle.type,
        capacity: vehicle.capacity,
        available,
        total,
        utilizationRate,
        availableCapacity: available * vehicle.capacity,
        // **Higher score = higher priority for utilization**
        priority: available * vehicle.capacity * (1 - utilizationRate)
      };
    })
    .filter(v => v.available > 0)
    .sort((a, b) => b.capacity - a.capacity); // Sort by capacity (largest first)

  if (fleetByCapacity.length === 0) {
    return 3; // Conservative fallback
  }

  // **NEW: Use largest available vehicle type as target**
  const largestAvailableVehicle = fleetByCapacity[0];

  // **NEW: Aggressive capacity targeting**
  let targetSize;
  if (largestAvailableVehicle.available >= 5) {
    // Plenty of vehicles available - target near full capacity
    targetSize = Math.max(4, largestAvailableVehicle.capacity - 1); // Leave 1 spot for potential guard
  } else if (largestAvailableVehicle.available >= 2) {
    // Moderate availability - target 75% capacity
    targetSize = Math.max(3, Math.floor(largestAvailableVehicle.capacity * 0.75));
  } else {
    // Low availability - target 60% capacity
    targetSize = Math.max(3, Math.floor(largestAvailableVehicle.capacity * 0.6));
  }

  console.log(`[Zone Capacity Aggressive] Zone ${zoneName}: Largest available: ${largestAvailableVehicle.type}(cap:${largestAvailableVehicle.capacity}, available:${largestAvailableVehicle.available}) → targeting ${targetSize}`);

  return targetSize;
}

function getAdaptiveGroupSize(availableFleetCounts, profile, targetSize) {
  // Find the largest available vehicle type
  const availableVehicles = profile.fleet
    .filter(v => (availableFleetCounts[v.type] || 0) > 0)
    .sort((a, b) => b.capacity - a.capacity);

  if (availableVehicles.length === 0) {
    return Math.min(targetSize, 3); // Conservative fallback
  }

  const largestAvailable = availableVehicles[0];
  const adaptedSize = Math.min(targetSize, largestAvailable.capacity - 1); // Leave room for guard

  if (adaptedSize < targetSize) {
    console.log(`[Adaptive Group Size] Reduced from ${targetSize} to ${adaptedSize} due to largest available vehicle: ${largestAvailable.type}(${largestAvailable.capacity})`);
  }

  return adaptedSize;
}


function getMaximumViableGroupSize(availableFleetCounts, profile, employees, activateGuardSystem = false) {
  // Find the largest available vehicle type
  const availableVehicles = profile.fleet
    .filter(v => (availableFleetCounts[v.type] || 0) > 0)
    .sort((a, b) => b.capacity - a.capacity);

  if (availableVehicles.length === 0) {
    return 3; // Conservative fallback
  }

  const largestAvailable = availableVehicles[0];

  // **Start with MAXIMUM capacity**
  let maxPossibleSize = largestAvailable.capacity;

  console.log(`[Maximum Capacity] Targeting FULL capacity: ${maxPossibleSize} for vehicle type ${largestAvailable.type}(${largestAvailable.capacity})`);

  return maxPossibleSize;
}

/**
 * Monitor and log fleet status during processing
 */
function logFleetStatus(availableFleetCounts, profile, context = "") {
  if (!profile.fleet) return;

  console.log(`\n[Fleet Status ${context}]`);
  let totalUsed = 0;
  let totalAvailable = 0;

  profile.fleet.forEach(fleetType => {
    const used = fleetType.count - (availableFleetCounts[fleetType.type] || 0);
    const remaining = availableFleetCounts[fleetType.type] || 0;
    const utilization = (used / fleetType.count * 100).toFixed(1);

    totalUsed += used;
    totalAvailable += fleetType.count;

    console.log(`  ${fleetType.type.toUpperCase()}: ${used}/${fleetType.count} used, ${remaining} remaining (${utilization}%)`);

    // Specific warnings for medium vehicles
    if (fleetType.type === 'm') {
      if (remaining > fleetType.count * 0.7) {
        console.warn(`  ⚠️  MEDIUM vehicles severely underutilized! Only ${utilization}% used.`);
      } else if (remaining > fleetType.count * 0.5) {
        console.warn(`  ⚠️  MEDIUM vehicles underutilized. ${utilization}% used.`);
      }
    }

    // Warning for small vehicle overuse
    if (fleetType.type === 's' && used > fleetType.count * 0.8) {
      console.warn(`  ⚠️  SMALL vehicles heavily used (${utilization}%). Consider using medium vehicles.`);
    }
  });

  const overallUtilization = (totalUsed / totalAvailable * 100).toFixed(1);
  console.log(`  OVERALL: ${totalUsed}/${totalAvailable} vehicles used (${overallUtilization}%)`);
  console.log('');
}

function calculateAngularDifferenceDegrees(bearingA, bearingB) {
  let diff = Math.abs(bearingA - bearingB);
  if (diff > 180) diff = 360 - diff;
  return diff;
}

function calculateHeuristicCandidateCost({
  currentLastEmpInPrelim,
  candidateEmp,
  facilityCoordinates,
  tripType,
  profile,
  maxNextStopDistanceKm,
}) {
  const directionPenaltyWeight = profile.heuristicDirectionPenaltyWeight || 5.0;
  const bearingPenaltyWeight = profile.heuristicBearingPenaltyWeight || 1.5;
  const facilityOrderPenaltyWeight = profile.heuristicFacilityOrderPenaltyWeight || 4.0;
  const distanceSpreadPenaltyWeight = profile.heuristicDistanceSpreadPenaltyWeight || 0.8;

  const distToLast = haversineDistance(
    [currentLastEmpInPrelim.location.lat, currentLastEmpInPrelim.location.lng],
    [candidateEmp.location.lat, candidateEmp.location.lng]
  );

  if (distToLast > maxNextStopDistanceKm) {
    return { feasible: false, reason: "max_next_stop_distance", cost: Infinity };
  }

  const distFromLastToFacility = haversineDistance(
    [currentLastEmpInPrelim.location.lat, currentLastEmpInPrelim.location.lng],
    facilityCoordinates
  );
  const distFromCandidateToFacility = haversineDistance(
    [candidateEmp.location.lat, candidateEmp.location.lng],
    facilityCoordinates
  );

  let directionalPenalty = 0;
  let facilityOrderPenalty = 0;
  if (tripType.toLowerCase() === "pickup") {
    const distanceIncrease = distFromCandidateToFacility - distFromLastToFacility;
    if (distanceIncrease > 0) {
      directionalPenalty = directionPenaltyWeight * distanceIncrease;
      facilityOrderPenalty = facilityOrderPenaltyWeight * distanceIncrease;
    }
  } else {
    const distanceDecrease = distFromLastToFacility - distFromCandidateToFacility;
    if (distanceDecrease > 0) {
      directionalPenalty = directionPenaltyWeight * distanceDecrease;
      facilityOrderPenalty = facilityOrderPenaltyWeight * distanceDecrease;
    }
  }

  const bearingFromLastToCandidate = calculateBearing(
    currentLastEmpInPrelim.location.lat,
    currentLastEmpInPrelim.location.lng,
    candidateEmp.location.lat,
    candidateEmp.location.lng
  );
  const bearingFromLastToFacility = calculateBearing(
    currentLastEmpInPrelim.location.lat,
    currentLastEmpInPrelim.location.lng,
    facilityCoordinates[0],
    facilityCoordinates[1]
  );

  const referenceBearing = tripType.toLowerCase() === "pickup"
    ? bearingFromLastToFacility
    : calculateBearing(
      facilityCoordinates[0],
      facilityCoordinates[1],
      currentLastEmpInPrelim.location.lat,
      currentLastEmpInPrelim.location.lng
    );
  const bearingDiff = calculateAngularDifferenceDegrees(bearingFromLastToCandidate, referenceBearing);
  const bearingPenalty = (bearingDiff / 180) * bearingPenaltyWeight * Math.max(distToLast, 0.5);

  const distanceSpreadPenalty =
    distanceSpreadPenaltyWeight * Math.abs(distFromCandidateToFacility - distFromLastToFacility);

  const totalCost =
    distToLast +
    directionalPenalty +
    facilityOrderPenalty +
    bearingPenalty +
    distanceSpreadPenalty;

  return {
    feasible: true,
    cost: totalCost,
    components: {
      distToLast,
      directionalPenalty,
      facilityOrderPenalty,
      bearingPenalty,
      distanceSpreadPenalty,
    },
  };
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
  shiftTime,
  fleetAnalysis
) {
  const routes = [];
  let employeesAddedToMasterUnroutedThisBatch = [];
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const facilityCoordinates = [facility.geoY, facility.geoX];
  const enableLocalSequenceOptimization = profile.enableLocalSequenceOptimization !== false;
  const localSequenceConfig = {
    maxIterations: profile.localSequenceMaxIterations || DEFAULT_LOCAL_SEQUENCE_MAX_ITERATIONS,
    directionPenaltyWeight:
      profile.localSequenceDirectionPenaltyWeight || DEFAULT_LOCAL_SEQUENCE_DIRECTION_PENALTY_WEIGHT,
    passengerBurdenWeight:
      profile.localSequencePassengerBurdenWeight || DEFAULT_LOCAL_SEQUENCE_PASSENGER_BURDEN_WEIGHT,
    monotonicSlackKm:
      profile.localSequenceMonotonicSlackKm || DEFAULT_LOCAL_SEQUENCE_MONOTONIC_SLACK_KM,
  };
  const deferredForInitialOSRM = [];

  const validEmployees = employees.filter(
    (emp) => emp.location?.lat != null && emp.location?.lng != null
  );

  // CRITICAL FIX: Employees without valid location should be added to unrouted pool
  const invalidLocationEmployees = employees.filter(
    (emp) => emp.location?.lat == null || emp.location?.lng == null
  );
  if (invalidLocationEmployees.length > 0) {
    console.warn(`[processEmployeeBatch] ${invalidLocationEmployees.length} employees have invalid/missing location. Adding to unrouted pool.`);
    employeesAddedToMasterUnroutedThisBatch.push(...invalidLocationEmployees);
  }

  if (validEmployees.length === 0) {
    return { routes, employeesAddedToMasterUnrouted: employeesAddedToMasterUnroutedThisBatch };
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
      isNMT: emp.isNMT || false,
    }))
    .sort((a, b) =>
      isDropoff
        ? a.distToFacility - b.distToFacility
        : b.distToFacility - a.distToFacility
    );

  let batchRouteCounter = 0;

  mainLoop: while (globalRemainingEmployees.length > 0) {
    batchRouteCounter++;

    const maxPossibleGroupSize = getMaximumViableGroupSize(
      availableFleetCounts,
      profile,
      globalRemainingEmployees,
      activateGuardSystem
    );

    let routeIsCurrentlySpecialNeedsHeuristic = false;

    const firstEmployeeForThisRoute = globalRemainingEmployees.shift();
    if (!firstEmployeeForThisRoute) break;

    let successfulRouteBuilt = false;
    let finalEmployeesForRoute = [];
    let finalRouteDetails = null;

    for (let attemptedGroupSize = maxPossibleGroupSize; attemptedGroupSize >= 1 && !successfulRouteBuilt; attemptedGroupSize--) {
      let currentHeuristicRouteMaxOccupancy = attemptedGroupSize;

      if (isSpecialNeedsUser(firstEmployeeForThisRoute)) {
        routeIsCurrentlySpecialNeedsHeuristic = true;
        currentHeuristicRouteMaxOccupancy = Math.min(currentHeuristicRouteMaxOccupancy, 2);
        console.log(`[Special Needs Constraint] Route ${batchRouteCounter}: Limited to 2 employees due to special needs user`);
      }

      let preliminaryEmployeesForThisAttempt = [firstEmployeeForThisRoute];
      let tempRemainingForThisAttempt = globalRemainingEmployees.filter(
        (e) => e.empCode !== firstEmployeeForThisRoute.empCode
      );

      // Use configurable max distance or default to 4km (increased from 2.25km to reduce singletons)
      const MAX_NEXT_STOP_DISTANCE_KM_HEURISTIC = profile.heuristicMaxStopDistance || 4.0;

      while (
        preliminaryEmployeesForThisAttempt.length < currentHeuristicRouteMaxOccupancy &&
        tempRemainingForThisAttempt.length > 0
      ) {
        const currentLastEmpInPrelim =
          preliminaryEmployeesForThisAttempt[preliminaryEmployeesForThisAttempt.length - 1];
        const candidatePool = [];

        tempRemainingForThisAttempt.forEach((candidateEmp, idx) => {
          const candidateIsSpecial = isSpecialNeedsUser(candidateEmp);

          if (routeIsCurrentlySpecialNeedsHeuristic && !candidateIsSpecial) return;
          if (
            !routeIsCurrentlySpecialNeedsHeuristic &&
            candidateIsSpecial &&
            preliminaryEmployeesForThisAttempt.length > 0 &&
            !isSpecialNeedsUser(preliminaryEmployeesForThisAttempt[0])
          ) return;

          const distToLast = haversineDistance(
            [currentLastEmpInPrelim.location.lat, currentLastEmpInPrelim.location.lng],
            [candidateEmp.location.lat, candidateEmp.location.lng]
          );

          // **LINEAR DIRECTION CHECK** - Ensure candidate is "on the way" (toward facility for PICKUP, away for DROP)
          // Use configurable tolerance (default 75 degrees allows some flexibility while preventing major detours)
          const linearToleranceDegrees = profile.heuristicLinearTolerance || 75;
          if (!isLinearDirection(currentLastEmpInPrelim, candidateEmp, facility, linearToleranceDegrees, tripType)) {
            // Candidate would cause zig-zag, skip unless it's very close (within 1km)
            if (distToLast > 1.0) return;
          }

          const heuristicCost = calculateHeuristicCandidateCost({
            currentLastEmpInPrelim,
            candidateEmp,
            facilityCoordinates,
            tripType,
            profile,
            maxNextStopDistanceKm: MAX_NEXT_STOP_DISTANCE_KM_HEURISTIC,
          });
          if (!heuristicCost.feasible) return;

          candidatePool.push({
            candidateEmp,
            idx,
            distToLast,
            heuristicCost: heuristicCost.cost,
          });
        });

        if (candidatePool.length === 0) break;

        candidatePool.sort((a, b) => a.heuristicCost - b.heuristicCost);

        const heuristicCandidatePoolSize = profile.heuristicCandidatePoolSize || 6;
        const heuristicOsrmEvaluationTopK = profile.heuristicOsrmEvaluationTopK || 3;
        const shortListedCandidates = candidatePool.slice(
          0,
          Math.min(heuristicCandidatePoolSize, candidatePool.length)
        );

        let chosenCandidateOutcome = null;
        const failedCandidateIndices = new Set();
        let osrmEvaluationsDone = 0;

        for (const candidateInfo of shortListedCandidates) {
          if (osrmEvaluationsDone >= heuristicOsrmEvaluationTopK) break;
          osrmEvaluationsDone++;

          const tentativePrelimEmployees = [
            ...preliminaryEmployeesForThisAttempt,
            candidateInfo.candidateEmp,
          ];
          const evaluatedPrelimEmployees = enableLocalSequenceOptimization
            ? optimizeEmployeeSequenceLocal(
              tentativePrelimEmployees,
              tripType,
              facilityCoordinates,
              localSequenceConfig
            )
            : tentativePrelimEmployees;

          const hasNMTInGroup = evaluatedPrelimEmployees.some(emp => emp.isNMT === true);
          if (hasNMTInGroup) {
            const nmtLimit = getNMTCapacityLimit(profile.fleet || []);
            if (evaluatedPrelimEmployees.length > nmtLimit) {
              continue;
            }
          }

          const tentativeCoords = evaluatedPrelimEmployees.map((emp) => [emp.location.lat, emp.location.lng]);
          const allTentativeCoords = isDropoff
            ? [facilityCoordinates, ...tentativeCoords]
            : [...tentativeCoords, facilityCoordinates];

          const tentativeDetails = await calculateRouteDetails(
            allTentativeCoords,
            evaluatedPrelimEmployees,
            pickupTimePerEmployee,
            tripType,
            city,
            shiftTime
          );

          if (tentativeDetails.error) {
            failedCandidateIndices.add(candidateInfo.idx);
            continue;
          }

          const serviceTime = evaluatedPrelimEmployees.length * pickupTimePerEmployee;
          const estimatedTotalDuration = tentativeDetails.totalDuration + serviceTime;

          if (maxDuration && estimatedTotalDuration > maxDuration) {
            continue;
          }

          const tempRouteForValidation = {
            employees: evaluatedPrelimEmployees,
            routeDetails: tentativeDetails,
            uniqueKey: `temp_val_progressive_${batchRouteCounter}_${attemptedGroupSize}`,
            tripType: tripType,
          };

          if (!(await checkRouteDeviation(tempRouteForValidation, facility, profile))) {
            continue;
          }

          const durationWeight = profile.heuristicDurationWeight || 0.0025;
          const blendedScore =
            candidateInfo.heuristicCost +
            durationWeight * estimatedTotalDuration;

          if (!chosenCandidateOutcome || blendedScore < chosenCandidateOutcome.blendedScore) {
            chosenCandidateOutcome = {
              bestCandidate: candidateInfo.candidateEmp,
              bestCandidateIndex: candidateInfo.idx,
              tentativeDetails,
              tentativePrelimEmployees: evaluatedPrelimEmployees,
              blendedScore,
            };
          }
        }

        if (!chosenCandidateOutcome) {
          const failedIdxDesc = Array.from(failedCandidateIndices)
            .filter((idx) => idx >= 0 && idx < tempRemainingForThisAttempt.length)
            .sort((a, b) => b - a);

          for (const failedIdx of failedIdxDesc) {
            tempRemainingForThisAttempt.splice(failedIdx, 1);
          }

          if (failedIdxDesc.length > 0) {
            continue;
          }
          break;
        }

        preliminaryEmployeesForThisAttempt.push(chosenCandidateOutcome.bestCandidate);
        tempRemainingForThisAttempt.splice(chosenCandidateOutcome.bestCandidateIndex, 1);

        if (isSpecialNeedsUser(chosenCandidateOutcome.bestCandidate) && !routeIsCurrentlySpecialNeedsHeuristic) {
          routeIsCurrentlySpecialNeedsHeuristic = true;
          currentHeuristicRouteMaxOccupancy = Math.min(currentHeuristicRouteMaxOccupancy, 2);
          console.log(`[Special Needs Update] Route ${batchRouteCounter}: Reduced max occupancy to 2 due to special needs employee added`);
        }
      }

      if (preliminaryEmployeesForThisAttempt.length > 0) {
        const finalEmployeesForAttempt = enableLocalSequenceOptimization
          ? optimizeEmployeeSequenceLocal(
            preliminaryEmployeesForThisAttempt,
            tripType,
            facilityCoordinates,
            localSequenceConfig
          )
          : preliminaryEmployeesForThisAttempt;
        const finalCoords = finalEmployeesForAttempt.map((emp) => [emp.location.lat, emp.location.lng]);
        const allFinalCoords = isDropoff
          ? [facilityCoordinates, ...finalCoords]
          : [...finalCoords, facilityCoordinates];

        const routeDetails = await calculateRouteDetails(
          allFinalCoords,
          finalEmployeesForAttempt,
          pickupTimePerEmployee,
          tripType,
          city,
          shiftTime
        );

        if (!routeDetails.error) {
          const tempRouteForFinalValidation = {
            employees: finalEmployeesForAttempt,
            routeDetails: routeDetails,
            uniqueKey: `temp_final_${batchRouteCounter}_${attemptedGroupSize}`,
            tripType: tripType,
          };

          const passesDeviation = await checkRouteDeviation(tempRouteForFinalValidation, facility, profile);
          const passesMaxDuration = !maxDuration || routeDetails.totalDuration <= maxDuration;

          if (passesDeviation && passesMaxDuration) {
            successfulRouteBuilt = true;
            finalEmployeesForRoute = finalEmployeesForAttempt;
            finalRouteDetails = routeDetails;

            const hasNMT = finalEmployeesForRoute.some(emp => emp.isNMT === true);
            const hasSpecialNeeds = finalEmployeesForRoute.some(emp => isSpecialNeedsUser(emp));
            const constraintInfo = [];
            if (hasNMT) constraintInfo.push("NMT");
            if (hasSpecialNeeds) constraintInfo.push("Special");
            if (constraintInfo.length > 0) {
              console.log(`[Route Success with Constraints] Route ${batchRouteCounter}: ${finalEmployeesForRoute.length} employees (${constraintInfo.join(", ")} constraints), duration: ${Math.round(routeDetails.totalDuration)}s`);
            } else {
              console.log(`[Route Success] Route ${batchRouteCounter}: ${finalEmployeesForRoute.length} employees, duration: ${Math.round(routeDetails.totalDuration)}s`);
            }

            const usedEmpCodes = new Set(finalEmployeesForRoute.map(e => e.empCode));
            globalRemainingEmployees = globalRemainingEmployees.filter(emp => !usedEmpCodes.has(emp.empCode));
            break;
          } else {
            if (!passesMaxDuration) {
              console.log(`[Duration Retry] Route ${batchRouteCounter}: Group size ${finalEmployeesForAttempt.length} failed duration check (${Math.round(routeDetails.totalDuration)}s > ${maxDuration}s). Trying smaller group.`);
            }
            if (!passesDeviation) {
              console.log(`[Deviation Retry] Route ${batchRouteCounter}: Group size ${finalEmployeesForAttempt.length} failed deviation check. Trying smaller group.`);
            }
          }
        }
      }
    }

    if (!successfulRouteBuilt) {
      deferredForInitialOSRM.push(firstEmployeeForThisRoute);
      continue;
    }

    if (activateGuardSystem && finalEmployeesForRoute.length > 0) {
      const isDropoff = tripType.toLowerCase() === "dropoff";
      const critIdx = isDropoff ? finalEmployeesForRoute.length - 1 : 0;
      const guardWillBeNeeded = finalEmployeesForRoute[critIdx]?.gender === "F";

      if (guardWillBeNeeded && finalEmployeesForRoute.length === maxPossibleGroupSize) {
        console.log(`[Guard Pre-Trim] Route ${batchRouteCounter}: Group is at full capacity (${finalEmployeesForRoute.length}) and needs a guard. Trimming one employee.`);

        const trimmedEmp = isDropoff ? finalEmployeesForRoute.shift() : finalEmployeesForRoute.pop();

        if (trimmedEmp) {
          globalRemainingEmployees.unshift(trimmedEmp);

          const finalCoords = finalEmployeesForRoute.map((emp) => [emp.location.lat, emp.location.lng]);
          const allFinalCoords = isDropoff
            ? [facilityCoordinates, ...finalCoords]
            : [...finalCoords, facilityCoordinates];

          finalRouteDetails = await calculateRouteDetails(
            allFinalCoords,
            finalEmployeesForRoute,
            pickupTimePerEmployee,
            tripType,
            city,
            shiftTime
          );
        }
      }
    }

    const routeShellForVehicleAssignment = {
      zone: firstEmployeeForThisRoute.zone,
      tripType: tripType,
      uniqueKey: `${firstEmployeeForThisRoute.zone}_batch_${batchRouteCounter}_${uuidv4()}`,
    };

    const { employeesTrimmedOff } = await assignVehicleAndFinalizeGroup(
      routeShellForVehicleAssignment,
      finalEmployeesForRoute,
      profile,
      availableFleetCounts,
      shiftTime,
      tripType,
      activateGuardSystem,
      fleetAnalysis
    );

    if (employeesTrimmedOff.length > 0) {
      employeesAddedToMasterUnroutedThisBatch.push(...employeesTrimmedOff);
      globalRemainingEmployees.unshift(...employeesTrimmedOff);
    }

    if (routeShellForVehicleAssignment.error || routeShellForVehicleAssignment.employees.length === 0) {
      // Route creation failed - add employees back to unrouted pool
      console.warn(`[processEmployeeBatch] Route creation failed for batch ${batchRouteCounter}. Adding ${finalEmployeesForRoute.length} employees to unrouted pool.`);
      employeesAddedToMasterUnroutedThisBatch.push(...finalEmployeesForRoute);
    } else {
      updateRouteWithDetails(routeShellForVehicleAssignment, finalRouteDetails);
      routes.push(routeShellForVehicleAssignment);
    }
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
        facilityData.profile?.directionPenaltyWeight || 5.0,
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
              `Failed to parse OR-Tools solution: ${e.message
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
    // console.log(`[RouteDeviation] BYPASSING deviation check for route ${route.uniqueKey || route.routeNumber} (global override).`);
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
        `[UnroutedDeviation] Route ${route.uniqueKey || route.routeNumber
        }: PASSED with minimal tolerance. Rule limit: ${ruleLimit}km, Actual: ${relevantRouteDistanceKm.toFixed(
          2
        )}km (exceeds by ${exceedanceKm.toFixed(
          2
        )}km). FarthestEmpDist: ${farthestEmployeeDistanceKm.toFixed(2)}km.`
      );
      return true;
    }

    console.warn(
      `[UnroutedDeviation] Route ${route.uniqueKey || route.routeNumber
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

async function calculateFarthestEmployeeDistance(route, facility, city, isDropoff) {
  let farthestEmployeeDistance = 0;
  if (!route || !route.employees || route.employees.length === 0 || !facility) {
    return farthestEmployeeDistance;
  }

  const fastApiCity = getFastApiCityKey(city);
  const farthestEmployee = isDropoff
    ? route.employees[route.employees.length - 1]
    : route.employees[0];

  if (farthestEmployee?.geoY && farthestEmployee?.geoX) {
    const coords = isDropoff
      ? [[facility.geoX, facility.geoY], [farthestEmployee.geoX, farthestEmployee.geoY]]
      : [[farthestEmployee.geoX, farthestEmployee.geoY], [facility.geoX, facility.geoY]];

    try {
      const response = await fetchApi(`https://mapapi.etmsonline.in/route`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          city: fastApiCity,
          coordinates: coords,
          overview: "false", steps: false, geometries: "polyline",
        }),
        timeout: OSRM_PROBE_TIMEOUT,
      });

      if (response.ok) {
        const data = await response.json();
        if (data.code === "Ok" && data.routes?.[0]?.distance != null) {
          farthestEmployeeDistance = data.routes[0].distance / 1000; // Convert to km
        }
      }
    } catch (err) {
      console.warn(`[FarthestDistance] OSRM error for route: ${err.message}`);
    }
  }
  return parseFloat(farthestEmployeeDistance.toFixed(2));
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
  reportingTime = 0,  // <-- ADD THIS PARAMETER
  fleetAnalysis
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
          `[Unrouted Consecutive] Employees ${emp1.empCode} and ${emp2.empCode
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
        `[Unrouted Singleton] Employee ${emp.empCode
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
      uniqueKey: `${emp.zone
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
      activateGuardSystemFromInput,
      fleetAnalysis
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
              // console.log(
              //   `[Unrouted Distance Check] Employee ${
              //     candidateEmp.empCode
              //   } is ${distance.toFixed(2)}km from ${
              //     existingEmp.empCode
              //   }. Too far for grouping.`
              // );
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
        uniqueKey: `${currentEmployeesInRouteAttempt[0].zone
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
          activateGuardSystemFromInput,
          fleetAnalysis
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
          `[Unrouted Success] Route ${routeForThisAttempt.uniqueKey
          } created with ${routeForThisAttempt.employees.length
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

// ==================== ROUTE CONSOLIDATION OPTIMIZATION ====================
// Constants for route consolidation
const DEFAULT_CONSOLIDATION_MAX_INSERTION_DISTANCE_KM = 2.5;
const DEFAULT_CONSOLIDATION_LOW_OCCUPANCY_THRESHOLD = 2;
const DEFAULT_CONSOLIDATION_MIN_HOST_OCCUPANCY = 3;
const DEFAULT_CROSS_ROUTE_MAX_ITERATIONS = 4;
const DEFAULT_CROSS_ROUTE_MAX_DETOUR_KM = 4.5;
const DEFAULT_CROSS_ROUTE_MAX_DETOUR_RATIO = 1.2;
const DEFAULT_CROSS_ROUTE_MAX_HEADING_DEVIATION_DEG = 100;
const DEFAULT_CROSS_ROUTE_MONOTONICITY_SLACK_KM = 1.8;
const DEFAULT_CROSS_ROUTE_CORRIDOR_RADIUS_KM = 5.0;
const DEFAULT_CROSS_ROUTE_TOP_HOST_CANDIDATES = 5;
const DEFAULT_CROSS_ROUTE_MAX_OSRM_CHECKS = 250;
const DEFAULT_CROSS_ROUTE_ROUTE_REDUCTION_BONUS_KM = 2.0;
const DEFAULT_CROSS_ROUTE_MIN_NET_GAIN_KM = -0.2;
const DEFAULT_CROSS_ROUTE_CANDIDATE_EMPLOYEES_PER_ROUTE = 6;
const DEFAULT_CROSS_ROUTE_OCCUPANCY_BONUS_KM = 0.8;
const DEFAULT_CROSS_ROUTE_PASSENGER_BURDEN_WEIGHT = 0.35;
const DEFAULT_LOCAL_SEQUENCE_MAX_ITERATIONS = 3;
const DEFAULT_LOCAL_SEQUENCE_DIRECTION_PENALTY_WEIGHT = 4.0;
const DEFAULT_LOCAL_SEQUENCE_PASSENGER_BURDEN_WEIGHT = 0.25;
const DEFAULT_LOCAL_SEQUENCE_MONOTONIC_SLACK_KM = 1.5;

/**
 * Find the best insertion point for an employee in an existing route
 * @param {Array} hostEmployees - Employees in the host route
 * @param {Object} candidateEmployee - Employee to insert
 * @param {string} tripType - 'pickup' or 'dropoff'
 * @param {Array} facilityCoordinates - [lat, lng] of facility
 * @returns {Object} - { insertionIndex, distanceImpact }
 */
function findBestInsertionPoint(hostEmployees, candidateEmployee, tripType, facilityCoordinates) {
  const isDropoff = tripType.toLowerCase() === 'dropoff';
  const candidateLoc = [candidateEmployee.location.lat, candidateEmployee.location.lng];
  const candidateDistToFacility = haversineDistance(candidateLoc, facilityCoordinates);

  let bestIndex = -1;
  let bestScore = Infinity;

  for (let i = 0; i <= hostEmployees.length; i++) {
    // Calculate the employee distances at positions before and after insertion point
    const prevEmp = i > 0 ? hostEmployees[i - 1] : null;
    const nextEmp = i < hostEmployees.length ? hostEmployees[i] : null;

    let prevDistToFacility = prevEmp
      ? haversineDistance([prevEmp.location.lat, prevEmp.location.lng], facilityCoordinates)
      : (isDropoff ? 0 : Infinity);

    let nextDistToFacility = nextEmp
      ? haversineDistance([nextEmp.location.lat, nextEmp.location.lng], facilityCoordinates)
      : (isDropoff ? Infinity : 0);

    // For pickup: employees should be ordered from farthest to closest to facility
    // For dropoff: employees should be ordered from closest to farthest from facility
    let directionalScore = 0;

    if (isDropoff) {
      // For dropoff, candidate should be between prev (closer) and next (farther)
      if (candidateDistToFacility >= prevDistToFacility && candidateDistToFacility <= nextDistToFacility) {
        directionalScore = 0; // Perfect placement
      } else {
        directionalScore = Math.abs(candidateDistToFacility - (prevDistToFacility + nextDistToFacility) / 2);
      }
    } else {
      // For pickup, candidate should be between prev (farther) and next (closer)
      if (candidateDistToFacility <= prevDistToFacility && candidateDistToFacility >= nextDistToFacility) {
        directionalScore = 0; // Perfect placement
      } else {
        directionalScore = Math.abs(candidateDistToFacility - (prevDistToFacility + nextDistToFacility) / 2);
      }
    }

    // Calculate detour distance
    let detourDistance = 0;
    if (prevEmp) {
      detourDistance += haversineDistance(
        [prevEmp.location.lat, prevEmp.location.lng],
        candidateLoc
      );
    }
    if (nextEmp) {
      detourDistance += haversineDistance(
        candidateLoc,
        [nextEmp.location.lat, nextEmp.location.lng]
      );
    }
    if (prevEmp && nextEmp) {
      // Subtract direct distance between prev and next
      detourDistance -= haversineDistance(
        [prevEmp.location.lat, prevEmp.location.lng],
        [nextEmp.location.lat, nextEmp.location.lng]
      );
    }

    // Combined score: prefer minimal detour and correct directional ordering
    const score = detourDistance + directionalScore * 2;

    if (score < bestScore) {
      bestScore = score;
      bestIndex = i;
    }
  }

  return {
    insertionIndex: bestIndex,
    distanceImpact: bestScore
  };
}

function getAngleDifferenceDeg(a, b) {
  const diff = Math.abs((a || 0) - (b || 0)) % 360;
  return diff > 180 ? 360 - diff : diff;
}

function getRelocationCandidateIndices(
  employees,
  tripType,
  facilityCoordinates,
  maxCandidates = DEFAULT_CROSS_ROUTE_CANDIDATE_EMPLOYEES_PER_ROUTE
) {
  const routeLength = employees?.length || 0;
  if (routeLength <= 0) return [];

  // For small/medium routes, evaluate every stop so middle points (e.g. #8/#9) are not skipped.
  if (routeLength <= 12) {
    return Array.from({ length: routeLength }, (_, i) => i);
  }

  const seed = [0, routeLength - 1, 1, routeLength - 2, Math.floor(routeLength / 2)];
  const withImpact = Array.from({ length: routeLength }, (_, idx) => ({
    idx,
    savingsKm: calculateRemovalSavingsKm(employees, idx, tripType, facilityCoordinates),
  })).sort((a, b) => b.savingsKm - a.savingsKm);

  const unique = [];
  for (const idx of seed) {
    if (idx >= 0 && idx < routeLength && !unique.includes(idx)) unique.push(idx);
  }

  for (const candidate of withImpact) {
    if (!unique.includes(candidate.idx)) unique.push(candidate.idx);
    if (unique.length >= Math.max(1, maxCandidates)) break;
  }

  return unique.slice(0, Math.max(1, maxCandidates));
}

function getMinDistanceToRouteEmployees(candidateEmployee, hostEmployees) {
  if (!candidateEmployee?.location || !hostEmployees?.length) return Infinity;
  let minDist = Infinity;
  const candidateLoc = [candidateEmployee.location.lat, candidateEmployee.location.lng];
  for (const emp of hostEmployees) {
    if (!emp?.location) continue;
    const dist = haversineDistance(candidateLoc, [emp.location.lat, emp.location.lng]);
    if (dist < minDist) minDist = dist;
  }
  return minDist;
}

function distancePointToSegmentKm(point, segStart, segEnd) {
  // Local equirectangular projection is sufficient for short urban segments.
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371;
  const [lat, lng] = point;
  const [lat1, lng1] = segStart;
  const [lat2, lng2] = segEnd;

  const meanLat = toRad((lat1 + lat2 + lat) / 3);
  const x = (toRad(lng) - toRad(lng1)) * Math.cos(meanLat) * R;
  const y = (toRad(lat) - toRad(lat1)) * R;
  const x2 = (toRad(lng2) - toRad(lng1)) * Math.cos(meanLat) * R;
  const y2 = (toRad(lat2) - toRad(lat1)) * R;

  const segLenSq = x2 * x2 + y2 * y2;
  if (segLenSq <= 1e-9) {
    return Math.sqrt(x * x + y * y);
  }

  const t = Math.max(0, Math.min(1, (x * x2 + y * y2) / segLenSq));
  const projX = t * x2;
  const projY = t * y2;
  const dx = x - projX;
  const dy = y - projY;
  return Math.sqrt(dx * dx + dy * dy);
}

function getMinDistanceToRouteCorridor(candidateEmployee, hostEmployees, tripType, facilityCoordinates) {
  if (!candidateEmployee?.location || !hostEmployees?.length) return Infinity;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const points = isDropoff
    ? [facilityCoordinates, ...hostEmployees.map((e) => [e.location.lat, e.location.lng])]
    : [...hostEmployees.map((e) => [e.location.lat, e.location.lng]), facilityCoordinates];

  if (points.length < 2) return Infinity;
  const point = [candidateEmployee.location.lat, candidateEmployee.location.lng];
  let minDist = Infinity;
  for (let i = 0; i < points.length - 1; i++) {
    const d = distancePointToSegmentKm(point, points[i], points[i + 1]);
    if (d < minDist) minDist = d;
  }
  return minDist;
}

function calculatePassengerBurdenPenaltyKm(hostEmployeeCount, insertionIndex, tripType, insertionDeltaKm) {
  if (!hostEmployeeCount || insertionDeltaKm <= 0) return 0;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  let impactedPassengers;
  if (isDropoff) {
    // In dropoff, earlier insertions impact more remaining passengers.
    impactedPassengers = Math.max(0, hostEmployeeCount - insertionIndex);
  } else {
    // In pickup, earlier insertions also increase ride time for downstream passengers.
    impactedPassengers = Math.max(0, hostEmployeeCount - insertionIndex);
  }
  return insertionDeltaKm * (impactedPassengers / Math.max(1, hostEmployeeCount));
}

function calculatePassengerBurdenDistanceKm(employees, tripType, facilityCoordinates) {
  if (!employees || employees.length === 0) return 0;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const points = isDropoff
    ? [facilityCoordinates, ...employees.map((e) => [e.location.lat, e.location.lng])]
    : [...employees.map((e) => [e.location.lat, e.location.lng]), facilityCoordinates];

  let burden = 0;
  const total = employees.length;
  for (let i = 0; i < points.length - 1; i++) {
    const legDist = haversineDistance(points[i], points[i + 1]);
    let onboard;
    if (isDropoff) {
      // facility->emp1 has n onboard, then n-1, ...
      onboard = Math.max(0, total - i);
    } else {
      // emp1->emp2 has 1 onboard, ..., last->facility has n onboard
      onboard = Math.min(total, i + 1);
    }
    burden += legDist * onboard;
  }
  return burden;
}

function calculateDirectionViolationKm(employees, tripType, facilityCoordinates, slackKm) {
  if (!employees || employees.length < 2) return 0;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const dists = employees.map((emp) =>
    haversineDistance([emp.location.lat, emp.location.lng], facilityCoordinates)
  );

  let violation = 0;
  for (let i = 0; i < dists.length - 1; i++) {
    if (isDropoff) {
      violation += Math.max(0, dists[i] - dists[i + 1] - slackKm);
    } else {
      violation += Math.max(0, dists[i + 1] - dists[i] - slackKm);
    }
  }
  return violation;
}

function calculateApproxRouteCostKm(
  employees,
  tripType,
  facilityCoordinates,
  {
    directionPenaltyWeight = DEFAULT_LOCAL_SEQUENCE_DIRECTION_PENALTY_WEIGHT,
    passengerBurdenWeight = DEFAULT_LOCAL_SEQUENCE_PASSENGER_BURDEN_WEIGHT,
    monotonicSlackKm = DEFAULT_LOCAL_SEQUENCE_MONOTONIC_SLACK_KM,
  } = {}
) {
  if (!employees || employees.length === 0) return 0;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const points = isDropoff
    ? [facilityCoordinates, ...employees.map((e) => [e.location.lat, e.location.lng])]
    : [...employees.map((e) => [e.location.lat, e.location.lng]), facilityCoordinates];

  let routeDistanceKm = 0;
  for (let i = 0; i < points.length - 1; i++) {
    routeDistanceKm += haversineDistance(points[i], points[i + 1]);
  }

  const passengerBurdenKm = calculatePassengerBurdenDistanceKm(
    employees,
    tripType,
    facilityCoordinates
  );
  const directionViolationKm = calculateDirectionViolationKm(
    employees,
    tripType,
    facilityCoordinates,
    monotonicSlackKm
  );

  return (
    routeDistanceKm +
    passengerBurdenWeight * passengerBurdenKm +
    directionPenaltyWeight * directionViolationKm
  );
}

function optimizeEmployeeSequenceLocal(
  employees,
  tripType,
  facilityCoordinates,
  {
    maxIterations = DEFAULT_LOCAL_SEQUENCE_MAX_ITERATIONS,
    directionPenaltyWeight = DEFAULT_LOCAL_SEQUENCE_DIRECTION_PENALTY_WEIGHT,
    passengerBurdenWeight = DEFAULT_LOCAL_SEQUENCE_PASSENGER_BURDEN_WEIGHT,
    monotonicSlackKm = DEFAULT_LOCAL_SEQUENCE_MONOTONIC_SLACK_KM,
  } = {}
) {
  if (!employees || employees.length < 3) return employees ? [...employees] : [];

  let current = employees.map((e) => ({ ...e }));
  let currentCost = calculateApproxRouteCostKm(current, tripType, facilityCoordinates, {
    directionPenaltyWeight,
    passengerBurdenWeight,
    monotonicSlackKm,
  });

  for (let iter = 0; iter < maxIterations; iter++) {
    let bestCost = currentCost;
    let bestSequence = null;

    // Relocate moves
    for (let i = 0; i < current.length; i++) {
      for (let j = 0; j <= current.length; j++) {
        if (j === i || j === i + 1) continue;
        const seq = [...current];
        const [moved] = seq.splice(i, 1);
        const insertAt = j > i ? j - 1 : j;
        seq.splice(insertAt, 0, moved);
        const cost = calculateApproxRouteCostKm(seq, tripType, facilityCoordinates, {
          directionPenaltyWeight,
          passengerBurdenWeight,
          monotonicSlackKm,
        });
        if (cost + 1e-6 < bestCost) {
          bestCost = cost;
          bestSequence = seq;
        }
      }
    }

    // Adjacent swaps
    for (let i = 0; i < current.length - 1; i++) {
      const seq = [...current];
      [seq[i], seq[i + 1]] = [seq[i + 1], seq[i]];
      const cost = calculateApproxRouteCostKm(seq, tripType, facilityCoordinates, {
        directionPenaltyWeight,
        passengerBurdenWeight,
        monotonicSlackKm,
      });
      if (cost + 1e-6 < bestCost) {
        bestCost = cost;
        bestSequence = seq;
      }
    }

    if (!bestSequence) break;
    current = bestSequence;
    currentCost = bestCost;
  }

  return current.map((e, idx) => ({ ...e, order: idx + 1 }));
}

function getRouteReferenceBearing(hostEmployees, tripType, facilityCoordinates) {
  if (!hostEmployees?.length) return null;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const refEmp = isDropoff ? hostEmployees[hostEmployees.length - 1] : hostEmployees[0];
  if (!refEmp?.location) return null;
  return calculateBearing(
    facilityCoordinates[0],
    facilityCoordinates[1],
    refEmp.location.lat,
    refEmp.location.lng
  );
}

function getPrevAndNextPointsForInsertion(hostEmployees, insertionIndex, tripType, facilityCoordinates) {
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const prevPoint = insertionIndex > 0
    ? [hostEmployees[insertionIndex - 1].location.lat, hostEmployees[insertionIndex - 1].location.lng]
    : (isDropoff ? facilityCoordinates : null);
  const nextPoint = insertionIndex < hostEmployees.length
    ? [hostEmployees[insertionIndex].location.lat, hostEmployees[insertionIndex].location.lng]
    : (!isDropoff ? facilityCoordinates : null);
  return { prevPoint, nextPoint };
}

function calculateInsertionDeltaKm(hostEmployees, candidateEmployee, insertionIndex, tripType, facilityCoordinates) {
  if (!candidateEmployee?.location) return Infinity;
  const candidatePoint = [candidateEmployee.location.lat, candidateEmployee.location.lng];
  const { prevPoint, nextPoint } = getPrevAndNextPointsForInsertion(
    hostEmployees,
    insertionIndex,
    tripType,
    facilityCoordinates
  );

  let oldDistance = 0;
  let newDistance = 0;

  if (prevPoint && nextPoint) {
    oldDistance = haversineDistance(prevPoint, nextPoint);
    newDistance =
      haversineDistance(prevPoint, candidatePoint) +
      haversineDistance(candidatePoint, nextPoint);
  } else if (prevPoint) {
    newDistance = haversineDistance(prevPoint, candidatePoint);
  } else if (nextPoint) {
    newDistance = haversineDistance(candidatePoint, nextPoint);
  }

  return Math.max(0, newDistance - oldDistance);
}

function calculateInsertionBaseSegmentKm(hostEmployees, insertionIndex, tripType, facilityCoordinates) {
  const { prevPoint, nextPoint } = getPrevAndNextPointsForInsertion(
    hostEmployees,
    insertionIndex,
    tripType,
    facilityCoordinates
  );
  if (!prevPoint || !nextPoint) return 0;
  return haversineDistance(prevPoint, nextPoint);
}

function calculateRemovalSavingsKm(sourceEmployees, removeIndex, tripType, facilityCoordinates) {
  if (!sourceEmployees?.length || removeIndex < 0 || removeIndex >= sourceEmployees.length) return 0;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const current = sourceEmployees[removeIndex];
  if (!current?.location) return 0;
  const currentPoint = [current.location.lat, current.location.lng];

  const prevPoint = removeIndex > 0
    ? [sourceEmployees[removeIndex - 1].location.lat, sourceEmployees[removeIndex - 1].location.lng]
    : (isDropoff ? facilityCoordinates : null);
  const nextPoint = removeIndex < sourceEmployees.length - 1
    ? [sourceEmployees[removeIndex + 1].location.lat, sourceEmployees[removeIndex + 1].location.lng]
    : (!isDropoff ? facilityCoordinates : null);

  let oldDistance = 0;
  let newDistance = 0;

  if (prevPoint) oldDistance += haversineDistance(prevPoint, currentPoint);
  if (nextPoint) oldDistance += haversineDistance(currentPoint, nextPoint);
  if (prevPoint && nextPoint) newDistance = haversineDistance(prevPoint, nextPoint);

  return Math.max(0, oldDistance - newDistance);
}

function calculateInsertionHeadingDeviationDeg(hostEmployees, candidateEmployee, insertionIndex, tripType, facilityCoordinates) {
  if (!candidateEmployee?.location) return 180;
  const candidatePoint = [candidateEmployee.location.lat, candidateEmployee.location.lng];
  const { prevPoint, nextPoint } = getPrevAndNextPointsForInsertion(
    hostEmployees,
    insertionIndex,
    tripType,
    facilityCoordinates
  );

  if (!prevPoint || !nextPoint) return 0;

  const baseBearing = calculateBearing(prevPoint[0], prevPoint[1], nextPoint[0], nextPoint[1]);
  const b1 = calculateBearing(prevPoint[0], prevPoint[1], candidatePoint[0], candidatePoint[1]);
  const b2 = calculateBearing(candidatePoint[0], candidatePoint[1], nextPoint[0], nextPoint[1]);
  return Math.max(getAngleDifferenceDeg(baseBearing, b1), getAngleDifferenceDeg(baseBearing, b2));
}

function isMonotonicRouteByFacilityDistance(employees, tripType, facilityCoordinates, slackKm = DEFAULT_CROSS_ROUTE_MONOTONICITY_SLACK_KM) {
  if (!employees || employees.length < 2) return true;
  const isDropoff = tripType.toLowerCase() === "dropoff";
  const dists = employees.map((emp) =>
    haversineDistance([emp.location.lat, emp.location.lng], facilityCoordinates)
  );

  for (let i = 0; i < dists.length - 1; i++) {
    if (isDropoff) {
      if (dists[i] - dists[i + 1] > slackKm) return false;
    } else {
      if (dists[i + 1] - dists[i] > slackKm) return false;
    }
  }
  return true;
}

async function optimizeRoutesAcrossRoutes(
  routes,
  facility,
  tripType,
  maxDuration,
  pickupTimePerEmployee,
  profile,
  city,
  shiftTime
) {
  const enableCrossRouteOptimization = profile.enableCrossRouteOptimization !== false;
  if (!enableCrossRouteOptimization) {
    console.log("[Cross-Route Optimization] Disabled via profile configuration.");
    return { routes, moveCount: 0, moves: [] };
  }

  const maxIterations = profile.crossRouteMaxIterations || DEFAULT_CROSS_ROUTE_MAX_ITERATIONS;
  const maxDetourKm = profile.crossRouteMaxDetourKm || DEFAULT_CROSS_ROUTE_MAX_DETOUR_KM;
  const maxDetourRatio = profile.crossRouteMaxDetourRatio || DEFAULT_CROSS_ROUTE_MAX_DETOUR_RATIO;
  const maxHeadingDeviationDeg = profile.crossRouteMaxHeadingDeviationDeg || DEFAULT_CROSS_ROUTE_MAX_HEADING_DEVIATION_DEG;
  const monotonicitySlackKm = profile.crossRouteMonotonicitySlackKm || DEFAULT_CROSS_ROUTE_MONOTONICITY_SLACK_KM;
  const corridorRadiusKm = profile.crossRouteCorridorRadiusKm || DEFAULT_CROSS_ROUTE_CORRIDOR_RADIUS_KM;
  const topHostCandidates = profile.crossRouteTopHostCandidates || DEFAULT_CROSS_ROUTE_TOP_HOST_CANDIDATES;
  const configuredMaxOsrmChecksPerIteration = profile.crossRouteMaxOsrmChecksPerIteration;
  const routeReductionBonusKm = profile.crossRouteRouteReductionBonusKm || DEFAULT_CROSS_ROUTE_ROUTE_REDUCTION_BONUS_KM;
  const occupancyBonusKm = profile.crossRouteOccupancyBonusKm || DEFAULT_CROSS_ROUTE_OCCUPANCY_BONUS_KM;
  const passengerBurdenWeight =
    profile.crossRoutePassengerBurdenWeight || DEFAULT_CROSS_ROUTE_PASSENGER_BURDEN_WEIGHT;
  const minNetGainKm =
    profile.crossRouteMinNetGainKm !== undefined
      ? profile.crossRouteMinNetGainKm
      : DEFAULT_CROSS_ROUTE_MIN_NET_GAIN_KM;
  const candidateEmployeesPerRoute =
    profile.crossRouteCandidateEmployeesPerRoute || DEFAULT_CROSS_ROUTE_CANDIDATE_EMPLOYEES_PER_ROUTE;

  const isDropoff = tripType.toLowerCase() === "dropoff";
  const facilityCoordinates = [facility.geoY, facility.geoX];
  const moves = [];
  const workingRoutes = routes.map((route) => ({
    ...route,
    employees: (route.employees || []).map((emp, idx) => ({ ...emp, order: idx + 1 })),
  }));

  console.log(
    `[Cross-Route Optimization] Starting with ${workingRoutes.length} routes. Config: iterations=${maxIterations}, maxDetourKm=${maxDetourKm}, maxDetourRatio=${maxDetourRatio}, corridorRadiusKm=${corridorRadiusKm}, maxHeadingDeviationDeg=${maxHeadingDeviationDeg}`
  );

  for (let iteration = 1; iteration <= maxIterations; iteration++) {
    let bestMove = null;
    let osrmChecks = 0;
    const deviationFailedHosts = new Set(); // routes that failed deviation this iteration — skip them for all future candidates

    const candidateRoutes = workingRoutes.filter(
      (r) => !r.error && r.employees && r.employees.length > 0
    ).sort((a, b) => (b.employees?.length || 0) - (a.employees?.length || 0));

    const dynamicOsrmBudget = Math.max(
      DEFAULT_CROSS_ROUTE_MAX_OSRM_CHECKS,
      candidateRoutes.length * Math.max(4, candidateEmployeesPerRoute * 2)
    );
    const maxOsrmChecksPerIteration = Math.max(
      configuredMaxOsrmChecksPerIteration || 0,
      dynamicOsrmBudget
    );
    console.log(
      `[Cross-Route Optimization] Iteration ${iteration}: candidateRoutes=${candidateRoutes.length}, osrmBudget=${maxOsrmChecksPerIteration} (configured=${configuredMaxOsrmChecksPerIteration || 0}, dynamic=${dynamicOsrmBudget})`
    );
    const rejectionStats = {
      capacity: 0,
      corridor: 0,
      bearing: 0,
      linear: 0,
      detour: 0,
      ratio: 0,
      heading: 0,
      monotonic: 0,
      constraints: 0,
      netGain: 0,
    };
    const constraintReasonCounts = {};

    for (const sourceRoute of candidateRoutes) {
      if (!sourceRoute?.employees?.length) continue;

      const sourceCandidateIndices = getRelocationCandidateIndices(
        sourceRoute.employees,
        tripType,
        facilityCoordinates,
        candidateEmployeesPerRoute
      );

      for (const sourceIndex of sourceCandidateIndices) {
        const candidateEmployee = sourceRoute.employees[sourceIndex];
        if (!candidateEmployee?.location?.lat || !candidateEmployee?.location?.lng) continue;

        const sourceRemovalSavingsKm = calculateRemovalSavingsKm(
          sourceRoute.employees,
          sourceIndex,
          tripType,
          facilityCoordinates
        );
        const candidateBearing = calculateBearing(
          facilityCoordinates[0],
          facilityCoordinates[1],
          candidateEmployee.location.lat,
          candidateEmployee.location.lng
        );

        const hostCandidates = [];
        for (const hostRoute of candidateRoutes) {
          if (!hostRoute?.employees?.length) continue;
          if (hostRoute.uniqueKey === sourceRoute.uniqueKey) continue;

          const currentOccupancy =
            hostRoute.employees.length + (hostRoute.guardNeeded ? 1 : 0);
          if (currentOccupancy >= hostRoute.vehicleCapacity) {
            rejectionStats.capacity++;
            continue;
          }

          const minDistanceToHostKm = getMinDistanceToRouteEmployees(
            candidateEmployee,
            hostRoute.employees
          );
          const minDistanceToCorridorKm = getMinDistanceToRouteCorridor(
            candidateEmployee,
            hostRoute.employees,
            tripType,
            facilityCoordinates
          );
          const effectiveProximityKm = Math.min(minDistanceToHostKm, minDistanceToCorridorKm);

          if (effectiveProximityKm > corridorRadiusKm) {
            rejectionStats.corridor++;
            continue;
          }

          const hostBearing = getRouteReferenceBearing(
            hostRoute.employees,
            tripType,
            facilityCoordinates
          );
          if (hostBearing != null) {
            const bearingDiff = getAngleDifferenceDeg(candidateBearing, hostBearing);
            if (
              bearingDiff > maxHeadingDeviationDeg &&
              effectiveProximityKm > corridorRadiusKm / 2
            ) {
              rejectionStats.bearing++;
              continue;
            }
          }

          const directionalAnchor = isDropoff
            ? hostRoute.employees[hostRoute.employees.length - 1]
            : hostRoute.employees[0];
          if (
            directionalAnchor &&
            !isLinearDirection(
              directionalAnchor,
              candidateEmployee,
              facility,
              maxHeadingDeviationDeg,
              tripType
            ) &&
            effectiveProximityKm > 1.5
          ) {
            rejectionStats.linear++;
            continue;
          }

          const insertionPoint = findBestInsertionPoint(
            hostRoute.employees,
            candidateEmployee,
            tripType,
            facilityCoordinates
          );
          if (insertionPoint.insertionIndex < 0) continue;

          const insertionDeltaKm = calculateInsertionDeltaKm(
            hostRoute.employees,
            candidateEmployee,
            insertionPoint.insertionIndex,
            tripType,
            facilityCoordinates
          );
          if (insertionDeltaKm > maxDetourKm) {
            rejectionStats.detour++;
            continue;
          }

          const baseSegmentKm = calculateInsertionBaseSegmentKm(
            hostRoute.employees,
            insertionPoint.insertionIndex,
            tripType,
            facilityCoordinates
          );
          if (baseSegmentKm > 0) {
            const detourRatio = insertionDeltaKm / baseSegmentKm;
            if (detourRatio > maxDetourRatio) {
              rejectionStats.ratio++;
              continue;
            }
          }

          const headingDeviationAtInsertion = calculateInsertionHeadingDeviationDeg(
            hostRoute.employees,
            candidateEmployee,
            insertionPoint.insertionIndex,
            tripType,
            facilityCoordinates
          );
          if (
            headingDeviationAtInsertion > maxHeadingDeviationDeg &&
            effectiveProximityKm > 1
          ) {
            rejectionStats.heading++;
            continue;
          }

          const tentativeEmployees = [...hostRoute.employees];
          tentativeEmployees.splice(insertionPoint.insertionIndex, 0, candidateEmployee);
          if (
            !isMonotonicRouteByFacilityDistance(
              tentativeEmployees,
              tripType,
              facilityCoordinates,
              monotonicitySlackKm
            )
          ) {
            rejectionStats.monotonic++;
            continue;
          }

          hostCandidates.push({
            hostKey: hostRoute.uniqueKey,
            insertionIndex: insertionPoint.insertionIndex,
            insertionDeltaKm,
            heuristicScore: insertionDeltaKm + effectiveProximityKm * 0.2,
          });
        }

        hostCandidates.sort((a, b) => a.heuristicScore - b.heuristicScore);
        const shortList = hostCandidates.slice(0, Math.max(1, topHostCandidates));

        for (const hostCandidate of shortList) {
          if (osrmChecks >= maxOsrmChecksPerIteration) break;
          if (deviationFailedHosts.has(hostCandidate.hostKey)) continue; // already proven over-deviation this iteration

          osrmChecks++;

          const hostRoute = workingRoutes.find((r) => r.uniqueKey === hostCandidate.hostKey);
          if (!hostRoute?.employees?.length) continue;

          const insertionResult = await canInsertEmployeeIntoRoute(
            hostRoute,
            candidateEmployee,
            hostCandidate.insertionIndex,
            facility,
            tripType,
            maxDuration,
            pickupTimePerEmployee,
            profile,
            city,
            shiftTime
          );
          if (!insertionResult.canInsert) {
            rejectionStats.constraints++;
            const reason = insertionResult.reason || "unknown";
            constraintReasonCounts[reason] =
              (constraintReasonCounts[reason] || 0) + 1;
            if (reason === 'deviation_exceeded') {
              deviationFailedHosts.add(hostCandidate.hostKey);
            }
            continue;
          }

          const sourceWillDisappear = sourceRoute.employees.length === 1;
          const hostOccupancyBefore =
            hostRoute.employees.length + (hostRoute.guardNeeded ? 1 : 0);
          const hostOccupancyAfter = hostOccupancyBefore + 1;
          const occupancyUplift =
            hostRoute.vehicleCapacity > 0
              ? hostOccupancyAfter / hostRoute.vehicleCapacity -
                hostOccupancyBefore / hostRoute.vehicleCapacity
              : 0;
          const occupancyReward = Math.max(0, occupancyUplift) * occupancyBonusKm;
          const passengerBurdenPenalty =
            passengerBurdenWeight *
            calculatePassengerBurdenPenaltyKm(
              hostRoute.employees.length,
              hostCandidate.insertionIndex,
              tripType,
              hostCandidate.insertionDeltaKm
            );
          const netGainKm =
            sourceRemovalSavingsKm -
            hostCandidate.insertionDeltaKm +
            (sourceWillDisappear ? routeReductionBonusKm : 0) +
            occupancyReward -
            passengerBurdenPenalty;
          if (netGainKm < minNetGainKm) {
            rejectionStats.netGain++;
            continue;
          }

          if (!bestMove || netGainKm > bestMove.netGainKm) {
            bestMove = {
              sourceKey: sourceRoute.uniqueKey,
              hostKey: hostRoute.uniqueKey,
              employeeCode: candidateEmployee.empCode,
              sourceRemovalSavingsKm,
              hostInsertionDeltaKm: hostCandidate.insertionDeltaKm,
              netGainKm,
              targetInsertionIndex: hostCandidate.insertionIndex,
            };
          }
        }

        if (osrmChecks >= maxOsrmChecksPerIteration) break;
      }

      if (osrmChecks >= maxOsrmChecksPerIteration) break;
    }

    if (!bestMove) {
      console.log(
        `[Cross-Route Optimization] Iteration ${iteration}: no improving move found (OSRM checks used: ${osrmChecks}/${maxOsrmChecksPerIteration}). Rejections=${JSON.stringify(rejectionStats)} ConstraintReasons=${JSON.stringify(constraintReasonCounts)}`
      );
      break;
    }

    const sourceIdx = workingRoutes.findIndex((r) => r.uniqueKey === bestMove.sourceKey);
    const hostIdx = workingRoutes.findIndex((r) => r.uniqueKey === bestMove.hostKey);
    if (sourceIdx === -1 || hostIdx === -1 || sourceIdx === hostIdx) {
      console.warn(
        `[Cross-Route Optimization] Iteration ${iteration}: invalid move targets, skipping.`
      );
      continue;
    }

    const sourceRoute = workingRoutes[sourceIdx];
    const hostRoute = workingRoutes[hostIdx];
    const movingEmployee = sourceRoute.employees.find(
      (emp) => emp.empCode === bestMove.employeeCode
    );

    if (!movingEmployee) {
      console.warn(
        `[Cross-Route Optimization] Iteration ${iteration}: employee ${bestMove.employeeCode} no longer in source route.`
      );
      continue;
    }

    if (!hostRoute.employees.some((emp) => emp.empCode === movingEmployee.empCode)) {
      const hostEmployees = [...hostRoute.employees];
      const safeInsertionIndex = Math.max(
        0,
        Math.min(bestMove.targetInsertionIndex, hostEmployees.length)
      );
      hostEmployees.splice(safeInsertionIndex, 0, movingEmployee);
      const optimizedHostEmployees =
        profile.enableLocalSequenceOptimization !== false
          ? optimizeEmployeeSequenceLocal(hostEmployees, tripType, facilityCoordinates, {
            maxIterations:
              profile.localSequenceMaxIterations || DEFAULT_LOCAL_SEQUENCE_MAX_ITERATIONS,
            directionPenaltyWeight:
              profile.localSequenceDirectionPenaltyWeight ||
              DEFAULT_LOCAL_SEQUENCE_DIRECTION_PENALTY_WEIGHT,
            passengerBurdenWeight:
              profile.localSequencePassengerBurdenWeight ||
              DEFAULT_LOCAL_SEQUENCE_PASSENGER_BURDEN_WEIGHT,
            monotonicSlackKm:
              profile.localSequenceMonotonicSlackKm ||
              DEFAULT_LOCAL_SEQUENCE_MONOTONIC_SLACK_KM,
          })
          : hostEmployees;
      hostRoute.employees = optimizedHostEmployees.map((emp, idx) => ({ ...emp, order: idx + 1 }));
      hostRoute.routeDetails = null;
      hostRoute.encodedPolyline = "";
    }

    const sourceAfterRemoval = sourceRoute.employees
      .filter((emp) => emp.empCode !== movingEmployee.empCode);
    const optimizedSourceEmployees =
      profile.enableLocalSequenceOptimization !== false
        ? optimizeEmployeeSequenceLocal(sourceAfterRemoval, tripType, facilityCoordinates, {
          maxIterations:
            profile.localSequenceMaxIterations || DEFAULT_LOCAL_SEQUENCE_MAX_ITERATIONS,
          directionPenaltyWeight:
            profile.localSequenceDirectionPenaltyWeight ||
            DEFAULT_LOCAL_SEQUENCE_DIRECTION_PENALTY_WEIGHT,
          passengerBurdenWeight:
            profile.localSequencePassengerBurdenWeight ||
            DEFAULT_LOCAL_SEQUENCE_PASSENGER_BURDEN_WEIGHT,
          monotonicSlackKm:
            profile.localSequenceMonotonicSlackKm ||
            DEFAULT_LOCAL_SEQUENCE_MONOTONIC_SLACK_KM,
        })
        : sourceAfterRemoval;
    sourceRoute.employees = optimizedSourceEmployees.map((emp, idx) => ({ ...emp, order: idx + 1 }));
    sourceRoute.routeDetails = null;
    sourceRoute.encodedPolyline = "";

    if (sourceRoute.employees.length === 0) {
      workingRoutes.splice(sourceIdx, 1);
    }

    moves.push(bestMove);
    console.log(
      `[Cross-Route Optimization] Iteration ${iteration}: moved ${bestMove.employeeCode} ${bestMove.sourceKey} -> ${bestMove.hostKey}, netGain=${bestMove.netGainKm.toFixed(
        3
      )}km (OSRM checks ${osrmChecks}/${maxOsrmChecksPerIteration})`
    );
  }

  console.log(
    `[Cross-Route Optimization] Complete. Applied ${moves.length} moves. Routes: ${routes.length} -> ${workingRoutes.length}`
  );

  return {
    routes: workingRoutes,
    moveCount: moves.length,
    moves,
  };
}

/**
 * Check if an employee can be inserted into a route without violating constraints
 */
async function canInsertEmployeeIntoRoute(
  hostRoute,
  candidateEmployee,
  insertionIndex,
  facility,
  tripType,
  maxDuration,
  pickupTimePerEmployee,
  profile,
  city,
  shiftTime
) {
  // Check capacity constraint
  const newOccupancy = hostRoute.employees.length + 1 + (hostRoute.guardNeeded ? 1 : 0);
  if (newOccupancy > hostRoute.vehicleCapacity) {
    return { canInsert: false, reason: 'capacity_exceeded' };
  }

  // Check NMT constraint
  const hasNMT = hostRoute.employees.some(e => e.isNMT) || candidateEmployee.isNMT;
  if (hasNMT) {
    const nmtLimit = getNMTCapacityLimit(profile.fleet || []);
    if (hostRoute.employees.length + 1 > nmtLimit) {
      return { canInsert: false, reason: 'nmt_limit' };
    }
  }

  // Check special needs constraint
  const hasSpecialNeeds = hostRoute.employees.some(isSpecialNeedsUser) || isSpecialNeedsUser(candidateEmployee);
  if (hasSpecialNeeds) {
    const specialLimit = hostRoute.guardNeeded ? 1 : 2;
    if (hostRoute.employees.length + 1 > specialLimit) {
      return { canInsert: false, reason: 'special_needs_limit' };
    }
  }

  // Create tentative employee list with insertion
  const tentativeEmployees = [...hostRoute.employees];
  tentativeEmployees.splice(insertionIndex, 0, candidateEmployee);
  const facilityCoordinates = [facility.geoY, facility.geoX];
  const optimizedTentativeEmployees =
    profile.enableLocalSequenceOptimization !== false
      ? optimizeEmployeeSequenceLocal(
        tentativeEmployees,
        tripType,
        facilityCoordinates,
        {
          maxIterations:
            profile.localSequenceMaxIterations || DEFAULT_LOCAL_SEQUENCE_MAX_ITERATIONS,
          directionPenaltyWeight:
            profile.localSequenceDirectionPenaltyWeight ||
            DEFAULT_LOCAL_SEQUENCE_DIRECTION_PENALTY_WEIGHT,
          passengerBurdenWeight:
            profile.localSequencePassengerBurdenWeight ||
            DEFAULT_LOCAL_SEQUENCE_PASSENGER_BURDEN_WEIGHT,
          monotonicSlackKm:
            profile.localSequenceMonotonicSlackKm ||
            DEFAULT_LOCAL_SEQUENCE_MONOTONIC_SLACK_KM,
        }
      )
      : tentativeEmployees;

  // Calculate new route details
  const isDropoff = tripType.toLowerCase() === 'dropoff';
  const employeeCoords = optimizedTentativeEmployees.map(e => [e.location.lat, e.location.lng]);
  const allCoords = isDropoff
    ? [facilityCoordinates, ...employeeCoords]
    : [...employeeCoords, facilityCoordinates];

  const tentativeDetails = await calculateRouteDetails(
    allCoords,
    optimizedTentativeEmployees,
    pickupTimePerEmployee,
    tripType,
    city,
    shiftTime
  );

  if (tentativeDetails.error) {
    return { canInsert: false, reason: 'osrm_error' };
  }

  // Check duration constraint
  const serviceTime = optimizedTentativeEmployees.length * pickupTimePerEmployee;
  const totalDuration = tentativeDetails.totalDuration + serviceTime;
  if (maxDuration && totalDuration > maxDuration) {
    return { canInsert: false, reason: 'duration_exceeded', actualDuration: totalDuration };
  }

  // Check deviation constraint
  const tentativeRoute = {
    employees: optimizedTentativeEmployees,
    routeDetails: tentativeDetails,
    uniqueKey: `consolidation_test_${hostRoute.uniqueKey}`,
    tripType: tripType
  };

  const passesDeviation = await checkRouteDeviation(tentativeRoute, facility, profile);
  if (!passesDeviation) {
    return { canInsert: false, reason: 'deviation_exceeded' };
  }

  return {
    canInsert: true,
    newEmployees: optimizedTentativeEmployees,
    newRouteDetails: tentativeDetails,
    totalDuration: totalDuration
  };
}

/**
 * Consolidate routes by merging low-occupancy routes into higher-occupancy routes
 * when employees are in the direction of the facility and can be added without
 * violating constraints.
 * 
 * @param {Array} routes - List of formed routes
 * @param {Object} facility - Facility data
 * @param {string} tripType - 'pickup' or 'dropoff'
 * @param {number} maxDuration - Maximum allowed route duration
 * @param {number} pickupTimePerEmployee - Time per employee stop
 * @param {Object} profile - Profile configuration
 * @param {string} city - City name for OSRM
 * @param {string} shiftTime - Shift time for traffic calculation
 * @returns {Object} - { routes: consolidatedRoutes, mergedRouteCount, mergeDetails }
 */
async function consolidateRoutes(
  routes,
  facility,
  tripType,
  maxDuration,
  pickupTimePerEmployee,
  profile,
  city,
  shiftTime
) {
  // Get configuration from profile or use defaults
  const maxInsertionDistanceKm = profile.consolidationMaxInsertionDistanceKm || DEFAULT_CONSOLIDATION_MAX_INSERTION_DISTANCE_KM;
  const lowOccupancyThreshold = profile.consolidationLowOccupancyThreshold || DEFAULT_CONSOLIDATION_LOW_OCCUPANCY_THRESHOLD;
  const minHostOccupancy = profile.consolidationMinHostOccupancy || DEFAULT_CONSOLIDATION_MIN_HOST_OCCUPANCY;
  const enableConsolidation = profile.enableRouteConsolidation !== false;

  if (!enableConsolidation) {
    console.log('[Route Consolidation] Disabled via profile configuration.');
    return { routes, mergedRouteCount: 0, mergeDetails: [] };
  }

  console.log(`\n[Route Consolidation] Starting consolidation phase...`);
  console.log(`[Route Consolidation] Config: maxInsertionDistance=${maxInsertionDistanceKm}km, lowOccupancyThreshold=${lowOccupancyThreshold}, minHostOccupancy=${minHostOccupancy}`);

  const facilityCoordinates = [facility.geoY, facility.geoX];
  const isDropoff = tripType.toLowerCase() === 'dropoff';

  // Separate routes into potential hosts and candidates for merging
  const validRoutes = routes.filter(r => !r.error && r.employees && r.employees.length > 0);

  // Identify low-occupancy routes (candidates for merging into others)
  const lowOccupancyRoutes = validRoutes.filter(r => r.employees.length <= lowOccupancyThreshold);

  // Identify potential host routes (higher occupancy but not at capacity)
  // **ENHANCED: Also allow low-occupancy routes to host if they have room (bi-directional merging)**
  const potentialHostRoutes = validRoutes.filter(r => {
    const currentOccupancy = r.employees.length + (r.guardNeeded ? 1 : 0);
    // Allow routes with at least minHostOccupancy OR any route with capacity
    // This enables bi-directional merging of small routes
    return (r.employees.length >= minHostOccupancy || profile.enableBidirectionalConsolidation) &&
      currentOccupancy < r.vehicleCapacity;
  });

  console.log(`[Route Consolidation] Found ${lowOccupancyRoutes.length} low-occupancy routes, ${potentialHostRoutes.length} potential host routes`);

  if (lowOccupancyRoutes.length === 0 || potentialHostRoutes.length === 0) {
    // **NEW: If no standard hosts, try to merge low-occupancy routes with each other**
    if (lowOccupancyRoutes.length >= 2 && profile.enableBidirectionalConsolidation !== false) {
      console.log('[Route Consolidation] Trying bi-directional merging of low-occupancy routes...');
    } else {
      console.log('[Route Consolidation] No consolidation opportunities found.');
      return { routes, mergedRouteCount: 0, mergeDetails: [] };
    }
  }

  const mergeDetails = [];
  const routesToRemove = new Set();
  const modifiedHostRoutes = new Map(); // Track which host routes have been modified
  const mergedEmployeeCodes = new Set(); // **CRITICAL: Track employees that have been merged to prevent duplicates**
  const sourceRoutesToUpdate = new Map(); // Track which employees to remove from source routes

  // Process each low-occupancy route
  for (const candidateRoute of lowOccupancyRoutes) {
    if (routesToRemove.has(candidateRoute.uniqueKey)) continue;

    // Try to merge all employees from this route into a host
    const employeesToMerge = [...candidateRoute.employees];
    let allEmployeesMerged = true;
    let targetHostRoute = null;
    const successfullyMergedFromThisRoute = [];

    for (const employee of employeesToMerge) {
      // Skip if this employee was already merged from a different route
      if (mergedEmployeeCodes.has(employee.empCode)) {
        console.log(`[Route Consolidation] ✗ Employee ${employee.empCode} already merged, skipping`);
        continue;
      }

      let bestHost = null;
      let bestInsertionResult = null;
      let bestScore = Infinity;

      // Find the best host route for this employee
      for (const hostRoute of potentialHostRoutes) {
        if (routesToRemove.has(hostRoute.uniqueKey)) continue;
        if (hostRoute.uniqueKey === candidateRoute.uniqueKey) continue;

        // Get current state of host route (may have been modified by previous merges)
        const currentHostState = modifiedHostRoutes.get(hostRoute.uniqueKey) || hostRoute;

        // Check if capacity allows another employee
        const currentOccupancy = currentHostState.employees.length + (currentHostState.guardNeeded ? 1 : 0);
        if (currentOccupancy >= hostRoute.vehicleCapacity) continue;

        // Check proximity to any employee in the host route
        let minDistanceToHost = Infinity;
        let closestHostEmp = null;
        for (const hostEmp of currentHostState.employees) {
          const dist = haversineDistance(
            [employee.location.lat, employee.location.lng],
            [hostEmp.location.lat, hostEmp.location.lng]
          );
          if (dist < minDistanceToHost) {
            minDistanceToHost = dist;
            closestHostEmp = hostEmp;
          }
        }

        if (minDistanceToHost > maxInsertionDistanceKm) continue;

        // **LINEAR DIRECTION CHECK** - Ensure merge maintains linear route (no zig-zag)
        if (closestHostEmp) {
          const linearTolerance = profile.consolidationLinearTolerance || 90;
          if (!isLinearDirection(closestHostEmp, employee, facility, linearTolerance, tripType)) {
            // Merging would create zig-zag, skip unless very close
            if (minDistanceToHost > 1.5) continue;
          }
        }

        // Find best insertion point
        const insertionPoint = findBestInsertionPoint(
          currentHostState.employees,
          employee,
          tripType,
          facilityCoordinates
        );

        // Check if insertion is valid
        const insertionResult = await canInsertEmployeeIntoRoute(
          currentHostState,
          employee,
          insertionPoint.insertionIndex,
          facility,
          tripType,
          maxDuration,
          pickupTimePerEmployee,
          profile,
          city,
          shiftTime
        );

        if (insertionResult.canInsert) {
          const score = insertionPoint.distanceImpact;
          if (score < bestScore) {
            bestScore = score;
            bestHost = hostRoute;
            bestInsertionResult = insertionResult;
          }
        }
      }

      if (bestHost && bestInsertionResult) {
        // Update the host route state
        const updatedHostState = {
          ...bestHost,
          employees: bestInsertionResult.newEmployees,
          routeDetails: bestInsertionResult.newRouteDetails
        };
        modifiedHostRoutes.set(bestHost.uniqueKey, updatedHostState);
        targetHostRoute = bestHost;

        // **CRITICAL: Mark this employee as merged to prevent duplicates**
        mergedEmployeeCodes.add(employee.empCode);
        successfullyMergedFromThisRoute.push(employee.empCode);

        console.log(`[Route Consolidation] ✓ Employee ${employee.empCode} from route ${candidateRoute.uniqueKey} → route ${bestHost.uniqueKey} (distance impact: ${bestScore.toFixed(2)}km)`);
      } else {
        allEmployeesMerged = false;
        console.log(`[Route Consolidation] ✗ Could not merge employee ${employee.empCode} from route ${candidateRoute.uniqueKey}`);
      }
    }

    // If all employees were merged, mark the candidate route for removal
    if (allEmployeesMerged && employeesToMerge.length > 0) {
      routesToRemove.add(candidateRoute.uniqueKey);
      mergeDetails.push({
        mergedRoute: candidateRoute.uniqueKey,
        intoRoute: targetHostRoute?.uniqueKey,
        employeeCount: employeesToMerge.length
      });
      console.log(`[Route Consolidation] ✓✓ Route ${candidateRoute.uniqueKey} fully merged into ${targetHostRoute?.uniqueKey}`);
    } else if (successfullyMergedFromThisRoute.length > 0) {
      // **CRITICAL: Track partial merges - these employees need to be removed from source route**
      sourceRoutesToUpdate.set(candidateRoute.uniqueKey, successfullyMergedFromThisRoute);
      console.log(`[Route Consolidation] ⚠ Route ${candidateRoute.uniqueKey} partially merged. Employees to remove: ${successfullyMergedFromThisRoute.join(', ')}`);
    }
  }

  // Build final route list
  const consolidatedRoutes = [];
  for (const route of routes) {
    if (routesToRemove.has(route.uniqueKey)) {
      continue; // Skip fully merged routes
    }

    // Use modified state if available (this route received employees)
    const modifiedState = modifiedHostRoutes.get(route.uniqueKey);

    // Check if this route had employees that were partially merged out
    const employeesToRemove = sourceRoutesToUpdate.get(route.uniqueKey);
    if (modifiedState) {
      // Reassign employee order numbers after merge
      const orderedEmployees = modifiedState.employees.map((emp, idx) => ({
        ...emp,
        order: idx + 1
      }));

      consolidatedRoutes.push({
        ...route,
        employees: orderedEmployees,
        routeDetails: modifiedState.routeDetails,
        // **CRITICAL: Update the encodedPolyline to match the new merged route**
        encodedPolyline: modifiedState.routeDetails.encodedPolyline,
        consolidatedFrom: mergeDetails.filter(m => m.intoRoute === route.uniqueKey).map(m => m.mergedRoute)
      });

      console.log(`[Route Consolidation] Final employee order for ${route.uniqueKey}: ${orderedEmployees.map(e => `${e.empCode}(#${e.order})`).join(', ')}`);
    } else if (employeesToRemove && employeesToRemove.length > 0) {
      // **CRITICAL: This route had some employees merged out - remove them to prevent duplicates**
      const remainingEmployees = route.employees
        .filter(emp => !employeesToRemove.includes(emp.empCode))
        .map((emp, idx) => ({ ...emp, order: idx + 1 }));

      if (remainingEmployees.length > 0) {
        // Recalculate route details for remaining employees
        const isDropoffTrip = tripType.toLowerCase() === 'dropoff';
        const employeeCoords = remainingEmployees.map(e => [e.location.lat, e.location.lng]);
        const allCoords = isDropoffTrip
          ? [facilityCoordinates, ...employeeCoords]
          : [...employeeCoords, facilityCoordinates];

        let newRouteDetails = route.routeDetails;
        try {
          newRouteDetails = await calculateRouteDetails(
            allCoords,
            remainingEmployees,
            pickupTimePerEmployee,
            tripType,
            city,
            shiftTime
          );
        } catch (err) {
          console.warn(`[Route Consolidation] Failed to recalculate route ${route.uniqueKey}: ${err.message}`);
        }

        consolidatedRoutes.push({
          ...route,
          employees: remainingEmployees,
          routeDetails: newRouteDetails,
          encodedPolyline: newRouteDetails.encodedPolyline || route.encodedPolyline,
          partiallyMerged: true
        });

        console.log(`[Route Consolidation] Route ${route.uniqueKey} after partial merge: ${remainingEmployees.map(e => e.empCode).join(', ')}`);
      } else {
        // All employees were actually merged, skip this route
        console.log(`[Route Consolidation] Route ${route.uniqueKey} has no remaining employees after partial merge, removing`);
      }
    } else {
      consolidatedRoutes.push(route);
    }
  }

  console.log(`[Route Consolidation] Complete. Merged ${routesToRemove.size} routes. Routes: ${routes.length} → ${consolidatedRoutes.length}`);

  // **CRITICAL: Final deduplication pass to ensure each employee appears in exactly one route**
  // This handles edge cases where routes are both sources and hosts
  const seenEmployees = new Set();
  const finalRoutes = [];
  const duplicatesRemoved = [];

  // Process routes in reverse order so that employees stay in their MERGED destination (later routes)
  for (let i = consolidatedRoutes.length - 1; i >= 0; i--) {
    const route = consolidatedRoutes[i];
    if (!route.employees || route.employees.length === 0) continue;

    const uniqueEmployees = [];
    for (const emp of route.employees) {
      if (!seenEmployees.has(emp.empCode)) {
        seenEmployees.add(emp.empCode);
        uniqueEmployees.push(emp);
      } else {
        duplicatesRemoved.push({ empCode: emp.empCode, fromRoute: route.uniqueKey });
      }
    }

    if (uniqueEmployees.length > 0) {
      // Reassign order numbers
      const orderedEmployees = uniqueEmployees.map((emp, idx) => ({ ...emp, order: idx + 1 }));
      finalRoutes.unshift({
        ...route,
        employees: orderedEmployees
      });
    }
  }

  if (duplicatesRemoved.length > 0) {
    console.log(`[Route Consolidation] **DEDUPLICATION** Removed ${duplicatesRemoved.length} duplicate employee appearances:`);
    duplicatesRemoved.forEach(d => console.log(`  - ${d.empCode} removed from ${d.fromRoute}`));
  }

  console.log(`[Route Consolidation] Final route count after deduplication: ${finalRoutes.length}`);

  return {
    routes: finalRoutes,
    mergedRouteCount: routesToRemove.size,
    mergeDetails
  };
}

// ==================== END ROUTE CONSOLIDATION OPTIMIZATION ====================

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

    // **NEW: Analyze fleet configuration**
    const fleetAnalysis = analyzeFleetConfiguration(profile.fleet);
    console.log(`[Fleet Analysis] Client fleet configuration analyzed:`, fleetAnalysis.capacityRanges);

    // **NEW: Initial fleet status**
    logFleetStatus(availableFleetCounts, profile, "Initial");

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

    // **UPDATED: Enhanced zone processing with fleet-aware capacity**
    const processZoneOrGroup = async (
      empsInScope,
      zoneIdentifier,
      targetHeuristicCapacity
    ) => {
      if (empsInScope.length === 0) return;

      // **NEW: Always target maximum possible capacity**
      const maxPossibleCapacity = getMaximumViableGroupSize(
        availableFleetCounts,
        profile,
        empsInScope,
        activateGuardSystemFromInput
      );

      const finalTargetCapacity = Math.max(targetHeuristicCapacity, maxPossibleCapacity);

      console.log(`[Zone Processing MAXIMUM] ${zoneIdentifier}: Targeting MAXIMUM capacity: ${finalTargetCapacity}`);

      const { routes: batchRoutes, employeesAddedToMasterUnrouted } =
        await processEmployeeBatch(
          empsInScope,
          finalTargetCapacity,
          facility,
          tripType,
          profileMaxDuration,
          pickupTimePerEmployee,
          activateGuardSystemFromInput,
          profile,
          availableFleetCounts,
          city,
          shiftTime,
          fleetAnalysis
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
    // In the main generateRoutes function, update the zone capacity logic:
    for (const [zoneName, zoneEmpList] of Object.entries(employeesByZone)) {
      if (processedZones.has(zoneName)) continue;
      const currentZoneEmployees = (zoneEmpList || []).filter(e => e.location);

      // **NEW: More aggressive initial capacity**
      let maxCap = getZoneCapacity(zoneName, profile);

      // **Scale up based on available fleet**
      const largestAvailableVehicle = profile.fleet
        .filter(v => (availableFleetCounts[v.type] || 0) > 0)
        .sort((a, b) => b.capacity - a.capacity)[0];

      if (largestAvailableVehicle) {
        maxCap = Math.max(maxCap, Math.floor(largestAvailableVehicle.capacity * 0.8));
      }

      console.log(`[Zone Initial Capacity] ${zoneName}: Base: ${getZoneCapacity(zoneName, profile)}, Fleet-adjusted: ${maxCap}`);

      await processZoneOrGroup(currentZoneEmployees, zoneName, maxCap);
    }

    // **NEW: Fleet status after zone processing**
    logFleetStatus(availableFleetCounts, profile, "After Zone Processing");

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

    // **NEW: Fleet status after OR-Tools**
    logFleetStatus(availableFleetCounts, profile, "After OR-Tools");

    // **NEW: Route Consolidation Phase**
    // Attempt to merge low-occupancy routes into higher-occupancy routes
    console.log(`\n[Route Consolidation] Pre-consolidation: ${allOptimizedOrToolsRoutes.length} routes`);

    const consolidationResult = await consolidateRoutes(
      allOptimizedOrToolsRoutes,
      facility,
      tripType,
      profileMaxDuration,
      pickupTimePerEmployee,
      profile,
      city,
      shiftTime
    );

    let routesAfterConsolidation = consolidationResult.routes;

    if (consolidationResult.mergedRouteCount > 0) {
      console.log(`[Route Consolidation] SUCCESS: Merged ${consolidationResult.mergedRouteCount} routes. Routes: ${allOptimizedOrToolsRoutes.length} → ${routesAfterConsolidation.length}`);
      console.log(`[Route Consolidation] Merge details:`, consolidationResult.mergeDetails);
    }

    // **NEW: Singleton Aggregation Phase**
    // Cluster any remaining singleton routes together based on direction and proximity
    const singletonAggregationRadius = profile.singletonAggregationRadius || 6; // km
    const singletonMaxClusterSize = profile.singletonMaxClusterSize || 4;

    const singletonCountBefore = routesAfterConsolidation.filter(r => r.employees?.length === 1).length;

    if (singletonCountBefore >= 2) {
      console.log(`\n[Singleton Aggregation] Pre-aggregation: ${singletonCountBefore} singleton routes`);
      routesAfterConsolidation = aggregateSingletonRoutes(
        routesAfterConsolidation,
        facility,
        tripType,
        singletonAggregationRadius,
        singletonMaxClusterSize
      );

      const singletonCountAfter = routesAfterConsolidation.filter(r => r.employees?.length === 1).length;
      console.log(`[Singleton Aggregation] Post-aggregation: ${singletonCountAfter} singleton routes (reduced by ${singletonCountBefore - singletonCountAfter})`);
    }

    // **NEW: Cross-route optimization phase**
    // Relocate employees between routes when it improves directional fit
    // and reduces avoidable detours without violating constraints.
    const crossRouteOptimizationResult = await optimizeRoutesAcrossRoutes(
      routesAfterConsolidation,
      facility,
      tripType,
      profileMaxDuration,
      pickupTimePerEmployee,
      profile,
      city,
      shiftTime
    );

    routesAfterConsolidation = crossRouteOptimizationResult.routes;

    if (crossRouteOptimizationResult.moveCount > 0) {
      console.log(
        `[Cross-Route Optimization] SUCCESS: Applied ${crossRouteOptimizationResult.moveCount} moves. Routes now: ${routesAfterConsolidation.length}`
      );
    }

    logFleetStatus(availableFleetCounts, profile, "After Route Consolidation");

    const finalProcessedRoutes = [];
    const collectedUnroutedForReinsertionMap = new Map();

    // Main route processing loop with guard handling
    for (const route of routesAfterConsolidation) {
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

      // --- Guard Swap Attempt ---
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
        // Guard capacity trimming logic would go here if needed
        // For now, this is handled in assignVehicleAndFinalizeGroup
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

    // **NEW: Fleet status before unrouted processing**
    logFleetStatus(availableFleetCounts, profile, "Before Unrouted Processing");

    // --- UNROUTED HANDLING WITH ITERATIVE TRIMMING ---
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
        reportingTime,
        fleetAnalysis
      );

      // Add successful routes to the container
      routeDataContainer.routeData.push(...unroutedResult.processedRoutes);
      totalRouteCount = unroutedResult.updatedRouteCount;

      // Update master unrouted pool with remaining
      masterUnroutedPool = unroutedResult.remainingUnrouted;
    }

    // --- FINAL FALLBACK: Create singleton routes for any remaining unrouted employees ---
    // This ensures NO employee is left unrouted
    if (masterUnroutedPool.length > 0) {
      console.log(`\n[FALLBACK SINGLETON ROUTING] ${masterUnroutedPool.length} employees still unrouted after all attempts. Creating singleton routes...`);

      const isDropoff = tripType.toLowerCase() === 'dropoff';
      const facilityCoordinates = [facility.geoY, facility.geoX];

      for (const employee of masterUnroutedPool) {
        totalRouteCount++;

        // Calculate route details for singleton
        const employeeCoords = [[employee.location?.lat || employee.geoY, employee.location?.lng || employee.geoX]];
        const allCoords = isDropoff
          ? [facilityCoordinates, ...employeeCoords]
          : [...employeeCoords, facilityCoordinates];

        let routeDetails = {
          totalDistance: 0,
          totalDuration: 0,
          encodedPolyline: '',
          legs: [],
          geometry: null
        };

        try {
          routeDetails = await calculateRouteDetails(
            allCoords,
            [employee],
            pickupTimePerEmployee,
            tripType,
            city,
            shiftTime
          );
        } catch (err) {
          console.warn(`[Fallback Singleton] Failed to calculate route for ${employee.empCode}: ${err.message}`);
        }

        // Create singleton route
        const singletonRoute = {
          zone: employee.zone || 'FALLBACK',
          tripType: tripType,
          uniqueKey: `FALLBACK_singleton_${employee.empCode}_${totalRouteCount}`,
          routeNumber: totalRouteCount,
          employees: [{
            ...employee,
            order: 1,
            location: employee.location || { lat: employee.geoY, lng: employee.geoX }
          }],
          routeDetails: routeDetails,
          encodedPolyline: routeDetails.encodedPolyline || '',
          vehicleCapacity: 4,
          assignedVehicleType: 's',
          guardNeeded: activateGuardSystemFromInput && employee.gender === 'F',
          isSpecialNeedsRoute: employee.isMedical || employee.isPWD || false,
          isNMTRoute: employee.isNMT || false,
          afterFleetExhaustion: true,
          error: false,
          fallbackSingleton: true
        };

        // Calculate pickup times for singleton
        calculatePickupTimes(singletonRoute, shiftTime, pickupTimePerEmployee, reportingTime);

        routeDataContainer.routeData.push(singletonRoute);
        console.log(`[Fallback Singleton] Created route ${totalRouteCount} for employee ${employee.empCode}`);
      }

      // Clear the unrouted pool since all have been routed
      masterUnroutedPool = [];
      console.log(`[FALLBACK SINGLETON ROUTING] Complete. All employees now routed.`);
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

    // Collect all employee codes that are currently routed
    const allEffectivelyRoutedEmpCodes = new Set();
    routeDataContainer.routeData.forEach((r) => {
      if (!r.error && r.employees)
        r.employees.forEach((e) => allEffectivelyRoutedEmpCodes.add(e.empCode));
    });

    // Find employees that are NOT routed
    let stillUnroutedEmployees = employees.filter(
      (emp) => !allEffectivelyRoutedEmpCodes.has(emp.empCode)
    );

    // --- FINAL COMPREHENSIVE FALLBACK: Create singleton routes for ANY remaining unrouted employees ---
    if (stillUnroutedEmployees.length > 0) {
      console.log(`\n[FINAL FALLBACK] ${stillUnroutedEmployees.length} employees still unrouted. Creating singleton routes for ALL...`);

      const isDropoff = tripType.toLowerCase() === 'dropoff';
      const facilityCoordinates = [facility.geoY, facility.geoX];
      let fallbackRouteCount = routeDataContainer.routeData.length;

      for (const employee of stillUnroutedEmployees) {
        fallbackRouteCount++;

        // Ensure employee has location data
        const empLat = employee.location?.lat || employee.geoY;
        const empLng = employee.location?.lng || employee.geoX;

        // Calculate route details for singleton
        const employeeCoords = [[empLat, empLng]];
        const allCoords = isDropoff
          ? [facilityCoordinates, ...employeeCoords]
          : [...employeeCoords, facilityCoordinates];

        let routeDetails = {
          totalDistance: 0,
          totalDuration: 0,
          encodedPolyline: '',
          legs: [],
          geometry: null
        };

        try {
          routeDetails = await calculateRouteDetails(
            allCoords,
            [{ ...employee, location: { lat: empLat, lng: empLng } }],
            pickupTimePerEmployee,
            tripType,
            city,
            shiftTime
          );
        } catch (err) {
          console.warn(`[Final Fallback] Failed to calculate route for ${employee.empCode}: ${err.message}`);
        }

        // Create singleton route
        const singletonRoute = {
          zone: employee.zone || 'FALLBACK',
          tripType: tripType,
          uniqueKey: `FINAL_FALLBACK_${employee.empCode}_${fallbackRouteCount}`,
          routeNumber: fallbackRouteCount,
          employees: [{
            ...employee,
            order: 1,
            location: { lat: empLat, lng: empLng }
          }],
          routeDetails: routeDetails,
          encodedPolyline: routeDetails.encodedPolyline || '',
          vehicleCapacity: 4,
          assignedVehicleType: 's',
          guardNeeded: activateGuardSystemFromInput && employee.gender === 'F',
          isSpecialNeedsRoute: employee.isMedical || employee.isPWD || false,
          isNMTRoute: employee.isNMT || false,
          afterFleetExhaustion: true,
          error: false,
          fallbackSingleton: true,
          finalFallback: true
        };

        // Calculate pickup times for singleton
        calculatePickupTimes(singletonRoute, shiftTime, pickupTimePerEmployee, reportingTime);

        routeDataContainer.routeData.push(singletonRoute);
        allEffectivelyRoutedEmpCodes.add(employee.empCode);
        console.log(`[Final Fallback] Created route ${fallbackRouteCount} for employee ${employee.empCode}`);
      }

      stillUnroutedEmployees = [];
      console.log(`[FINAL FALLBACK] Complete. All ${employees.length} employees are now routed.`);

      // Regenerate response with the new routes added
      const updatedStats = calculateRouteStatistics(routeDataContainer, employees.length);
      Object.assign(response, await createSimplifiedResponse({
        ...routeDataContainer,
        ...updatedStats,
        totalSwappedRoutes: finalTotalSwappedRoutes,
      }));
    }

    // Final unrouted check (should be empty now)
    response.unroutedEmployees = employees
      .filter((emp) => !allEffectivelyRoutedEmpCodes.has(emp.empCode))
      .map((emp) => ({
        empCode: emp.empCode,
        geoX: emp.geoX,
        geoY: emp.geoY,
        gender: emp.gender,
        isMedical: emp.isMedical || false,
        isPWD: emp.isPWD || false,
      }));

    // **NEW: Final fleet utilization summary**
    console.log('\n=== FINAL FLEET UTILIZATION SUMMARY ===');
    let totalVehiclesUsed = 0;
    let totalVehiclesAvailable = 0;

    if (profile.fleet && Array.isArray(profile.fleet)) {
      profile.fleet.forEach(fleetType => {
        const used = fleetType.count - (availableFleetCounts[fleetType.type] || 0);
        const utilization = (used / fleetType.count * 100).toFixed(1);
        totalVehiclesUsed += used;
        totalVehiclesAvailable += fleetType.count;

        let status = '';
        if (fleetType.type === 'm' && used < fleetType.count * 0.3) {
          status = ' ⚠️ UNDERUTILIZED';
        } else if (fleetType.type === 's' && used > fleetType.count * 0.8) {
          status = ' ⚠️ OVERUTILIZED';
        } else if (fleetType.type === 'm' && used > fleetType.count * 0.6) {
          status = ' ✅ WELL UTILIZED';
        }

        console.log(`${fleetType.type.toUpperCase()} type (cap:${fleetType.capacity}): ${used}/${fleetType.count} used (${utilization}% utilization)${status}`);
      });

      const overallUtilization = (totalVehiclesUsed / totalVehiclesAvailable * 100).toFixed(1);
      console.log(`OVERALL FLEET: ${totalVehiclesUsed}/${totalVehiclesAvailable} vehicles used (${overallUtilization}% utilization)`);
    }
    console.log('==========================================\n');

    // **NEW: Add fleet utilization to response**
    response.fleetUtilization = {
      totalVehiclesUsed,
      totalVehiclesAvailable,
      overallUtilization: parseFloat((totalVehiclesUsed / totalVehiclesAvailable * 100).toFixed(1)),
      byType: profile.fleet ? profile.fleet.map(fleetType => {
        const used = fleetType.count - (availableFleetCounts[fleetType.type] || 0);
        return {
          type: fleetType.type,
          capacity: fleetType.capacity,
          total: fleetType.count,
          used: used,
          remaining: availableFleetCounts[fleetType.type] || 0,
          utilization: parseFloat((used / fleetType.count * 100).toFixed(1))
        };
      }) : []
    };

    // **NEW: Add route consolidation stats to response**
    response.routeConsolidation = {
      enabled: profile.enableRouteConsolidation !== false,
      mergedRouteCount: consolidationResult.mergedRouteCount,
      routesSavedByConsolidation: consolidationResult.mergedRouteCount,
      mergeDetails: consolidationResult.mergeDetails
    };

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

    let TRAFFIC_BUFFER_PERCENTAGE = getTrafficBufferForShiftTime(shiftTime);
    TRAFFIC_BUFFER_PERCENTAGE = Math.min(TRAFFIC_BUFFER_PERCENTAGE, 0.8); // Cap buffer

    const timeStr = shiftTime.toString().padStart(4, "0");
    const hours = parseInt(timeStr.substring(0, 2), 10);
    const minutes = parseInt(timeStr.substring(2, 4), 10);

    if (
      isNaN(hours) || isNaN(minutes) || hours < 0 || hours > 23 || minutes < 0 || minutes > 59
    ) {
      throw new Error(`Invalid shift time format: ${shiftTime}`);
    }

    const facilityTargetTime = new Date();
    facilityTargetTime.setHours(hours, minutes, 0, 0);
    const isDropoff = route.tripType?.toLowerCase() === "dropoff";

    let routeStartTime, routeEndTime; // To calculate final duration

    if (!isDropoff) {
      // PICKUP LOGIC
      let targetFacilityArrivalTime = new Date(
        facilityTargetTime.getTime() - reportingTimeSeconds * 1000
      );
      routeEndTime = targetFacilityArrivalTime; // Route ends at facility
      route.facilityArrivalTime = formatTime(targetFacilityArrivalTime);

      let currentTimeMs = targetFacilityArrivalTime.getTime();

      for (let i = route.employees.length - 1; i >= 0; i--) {
        const employee = route.employees[i];
        let legDuration = 0;

        // Correctly get duration for the leg FROM this employee TO the next point
        if (i === route.employees.length - 1) {
          // Last employee: travel time to facility is the last leg
          const lastLegIndex = route.routeDetails?.legs?.length - 1;
          if (lastLegIndex >= 0) {
            legDuration =
              route.routeDetails.legs[lastLegIndex]?.duration || 0;
          }
        } else {
          // Other employees: travel time to the next employee is leg[i]
          legDuration = route.routeDetails?.legs?.[i]?.duration || 0;
        }

        const bufferedLegDuration =
          legDuration * (1 + TRAFFIC_BUFFER_PERCENTAGE);

        // Subtract travel time
        currentTimeMs -= bufferedLegDuration * 1000;

        // Subtract service time for this employee
        currentTimeMs -= pickupTimePerEmployee * 1000;

        const pickupTime = new Date(currentTimeMs);
        employee.pickupTime = formatTime(pickupTime);

        if (i === 0) {
          routeStartTime = pickupTime; // First pickup is the route start
        }
      }
    } else {
      // DROPOFF LOGIC
      routeStartTime = facilityTargetTime; // Route starts at facility
      route.facilityDepartureTime = formatTime(facilityTargetTime);
      let currentTimeMs = facilityTargetTime.getTime();

      for (let i = 0; i < route.employees.length; i++) {
        const employee = route.employees[i];
        // Travel TO this employee from previous point is leg[i]
        const legDuration = route.routeDetails?.legs?.[i]?.duration || 0;
        const bufferedLegDuration =
          legDuration * (1 + TRAFFIC_BUFFER_PERCENTAGE);

        // Add travel time
        currentTimeMs += bufferedLegDuration * 1000;

        // Add service time (dropoff)
        currentTimeMs += pickupTimePerEmployee * 1000;

        const dropoffTime = new Date(currentTimeMs);
        employee.dropoffTime = formatTime(dropoffTime);
        employee.pickupTime = employee.dropoffTime; // legacy compatibility

        if (i === route.employees.length - 1) {
          routeEndTime = dropoffTime; // Last dropoff is route end
        }
      }
    }

    // Recalculate and update totalDuration to be consistent with ETAs
    if (routeStartTime && routeEndTime && route.routeDetails) {
      const originalOsrmDuration = route.routeDetails.totalDuration;
      const newDurationInSeconds = Math.round(
        (routeEndTime.getTime() - routeStartTime.getTime()) / 1000
      );

      if (newDurationInSeconds >= 0) {
        // console.log(
        //   `[Duration Sync] Route ${
        //     route.uniqueKey
        //   }: Original OSRM duration (travel only): ${originalOsrmDuration.toFixed(
        //     0
        //   )}s. New ETA-based duration (travel + service): ${newDurationInSeconds}s.`
        // );
        // Overwrite the old duration with the new, more accurate one.
        route.routeDetails.totalDuration = newDurationInSeconds;
      }
    }
  } catch (error) {
    console.error("Time calculation error:", error.message, error.stack);
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
          )} <= ${MAX_SWAP_DISTANCE_KM} is ${roadDistanceKm <= MAX_SWAP_DISTANCE_KM
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
        `[GuardSwap - Experiential] Route ${route.uniqueKey
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
    `Assigning error state to route ${route.uniqueKey || route.routeNumber || "UNKNOWN"
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

  // Renumber routes sequentially to eliminate any gaps
  processedRoutes.forEach((route, index) => {
    route.routeNumber = index + 1;
  });

  return {
    uuid: routeDataContainer.uuid,
    date: routeDataContainer.date,
    shift: routeDataContainer.shift,
    tripType: routeDataContainer.tripType === "PICKUP" ? "P" : "D",
    totalEmployees: routeDataContainer.totalEmployees,
    totalRoutedEmployees: routeDataContainer.totalRoutedEmployees,
    totalRoutes: processedRoutes.length,
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

async function calculateEta(currentGeocodes, destinationGeocodes, shiftTime, city) {
  const fastApiCity = getFastApiCityKey(city);
  const trafficBuffer = getTrafficBufferForShiftTime(shiftTime);

  const coordinates = [
    [currentGeocodes.lng, currentGeocodes.lat],
    [destinationGeocodes.lng, destinationGeocodes.lat]
  ];

  try {
    const response = await fetchApi(`https://mapapi.etmsonline.in/table`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        city: fastApiCity,
        coordinates: coordinates,
        annotations: 'duration'
      })
    });

    if (!response.ok) {
      throw new Error(`OSRM API responded with status ${response.status}`);
    }

    const data = await response.json();

    if (data.code === 'Ok' && data.durations && data.durations.length > 1) {
      const baseDuration = data.durations[0][1];
      return Math.round(baseDuration * (1 + trafficBuffer));
    } else {
      throw new Error('Invalid response from OSRM API');
    }
  } catch (error) {
    console.error('Error in calculateEta:', error);
    return -1; // Return -1 or handle the error as you see fit
  }
}

/**
 * Reoptimize a route using OR-Tools VRP solver to find optimal employee order
 * Then calculate route details (ETA, polyline) for the optimized order
 * 
 * @param {Object} params - Parameters for route reoptimization
 * @param {Array} params.employees - List of employees with geoX, geoY coordinates
 * @param {Object} params.facility - Facility with geoX, geoY coordinates
 * @param {string} params.tripType - 'PICKUP' or 'DROPOFF'
 * @param {number} params.vehicleCapacity - Max employees per vehicle (default: employees.length)
 * @param {number} params.pickupTimePerEmployee - Time per pickup in seconds
 * @param {string} params.city - City for OSRM routing
 * @param {string|number} params.shiftTime - Shift time for traffic calculation
 * @param {number} params.maxRouteDuration - Max route duration in seconds (optional)
 * @returns {Object} - Optimized employees with route details
 */
/**
 * Reoptimize a route using INSERTION-BASED algorithm
 * This preserves the existing employee order and finds optimal positions for new employees
 * @param {Object} params - Configuration parameters
 * @returns {Object} - Optimized employees with route details
 */
async function reoptimizeRouteWithVRP({
  employees,
  facility,
  tripType,
  vehicleCapacity,
  pickupTimePerEmployee,
  city,
  shiftTime,
  maxRouteDuration = 7200
}) {
  if (!employees || employees.length === 0) {
    return {
      reOptimized: false,
      employees: [],
      error: "No employees provided"
    };
  }

  // For single employee, no optimization needed
  if (employees.length === 1) {
    const facilityCoordinates = [facility.geoY, facility.geoX];
    const empCoords = [[employees[0].geoY, employees[0].geoX]];
    const routeCoords = tripType.toUpperCase() === 'DROPOFF'
      ? [facilityCoordinates, ...empCoords]
      : [...empCoords, facilityCoordinates];

    const routeDetails = await calculateRouteDetails(
      routeCoords,
      employees,
      pickupTimePerEmployee,
      tripType,
      city,
      shiftTime
    );

    return {
      reOptimized: true,
      employees: routeDetails.employees || employees,
      routeDetails
    };
  }

  const facilityLocation = { lat: facility.geoY, lng: facility.geoX };
  const isPickup = tripType.toUpperCase() === 'PICKUP';

  try {
    // Separate existing employees (in order) from new employees
    const existingEmployees = employees.filter(emp => !emp.isNewlyAdded);
    const newEmployees = employees.filter(emp => emp.isNewlyAdded);

    console.log(`[RE-OPTIMIZE] Insertion mode: ${existingEmployees.length} existing, ${newEmployees.length} new employees`);

    // If no new employees, just recalculate route details with existing order
    if (newEmployees.length === 0) {
      console.log(`[RE-OPTIMIZE] No new employees, keeping existing order`);
      const empCoords = existingEmployees.map(emp => [emp.geoY, emp.geoX]);
      const routeCoords = isPickup
        ? [...empCoords, [facility.geoY, facility.geoX]]
        : [[facility.geoY, facility.geoX], ...empCoords];

      const routeDetails = await calculateRouteDetails(
        routeCoords,
        existingEmployees,
        pickupTimePerEmployee,
        tripType,
        city,
        shiftTime
      );

      return {
        reOptimized: true,
        employees: routeDetails.employees || existingEmployees,
        routeDetails
      };
    }

    // If all employees are new (no existing order to preserve), use full VRP
    if (existingEmployees.length === 0) {
      console.log(`[RE-OPTIMIZE] All employees are new, using VRP for initial optimization`);
      return await reoptimizeRouteWithFullVRP({
        employees,
        facility,
        tripType,
        vehicleCapacity,
        pickupTimePerEmployee,
        city,
        shiftTime,
        maxRouteDuration
      });
    }

    // Normalize employees to have location object
    const allEmployees = [...existingEmployees, ...newEmployees];
    const normalizedEmployees = allEmployees.map(emp => ({
      ...emp,
      location: emp.location || { lat: emp.geoY, lng: emp.geoX }
    }));

    console.log(`[DEBUG-COORDS] Facility: lat=${facilityLocation.lat}, lng=${facilityLocation.lng}`);
    console.log(`[DEBUG-COORDS] First employee: ${normalizedEmployees[0]?.empCode}, lat=${normalizedEmployees[0]?.location?.lat}, lng=${normalizedEmployees[0]?.location?.lng}`);
    console.log(`[DEBUG-COORDS] Last employee: ${normalizedEmployees[normalizedEmployees.length - 1]?.empCode}, lat=${normalizedEmployees[normalizedEmployees.length - 1]?.location?.lat}, lng=${normalizedEmployees[normalizedEmployees.length - 1]?.location?.lng}`);

    // Generate distance/duration matrix using OSRM
    let matrixData = await generateDistanceDurationMatrix(
      normalizedEmployees,
      facilityLocation,
      city
    );

    let { distanceMatrix, durationMatrix, pointMap } = matrixData;

    if (!distanceMatrix || distanceMatrix.length === 0) {
      return {
        reOptimized: false,
        employees,
        error: "Failed to generate distance matrix"
      };
    }

    // Check if OSRM returned all zeros (fallback to Haversine)
    const osrmHasValidData = distanceMatrix[0]?.[1] > 0 || distanceMatrix[1]?.[0] > 0;
    if (!osrmHasValidData) {
      console.log(`[RE-OPTIMIZE] OSRM returned zeros, falling back to Haversine distances`);

      // Build coordinates array: [facility, ...employees]
      const allCoords = [
        [facilityLocation.lat, facilityLocation.lng],
        ...normalizedEmployees.map(emp => [emp.location.lat, emp.location.lng])
      ];

      // Generate Haversine distance matrix (in meters to match OSRM format)
      const n = allCoords.length;
      distanceMatrix = [];
      for (let i = 0; i < n; i++) {
        distanceMatrix[i] = [];
        for (let j = 0; j < n; j++) {
          if (i === j) {
            distanceMatrix[i][j] = 0;
          } else {
            // haversineDistance returns km, convert to meters
            distanceMatrix[i][j] = haversineDistance(allCoords[i], allCoords[j]) * 1000;
          }
        }
      }

      console.log(`[DEBUG-HAVERSINE] Generated ${n}x${n} matrix, sample [0][1]=${distanceMatrix[0]?.[1]?.toFixed(0)}m`);
    }

    // Build index mapping for the matrix
    // Index 0 = facility, Index 1..N = employees in order of normalizedEmployees
    const empToMatrixIndex = new Map();
    normalizedEmployees.forEach((emp, idx) => {
      empToMatrixIndex.set(emp.empCode, idx + 1); // +1 because index 0 is facility
    });

    console.log(`[DEBUG-MATRIX] Matrix size: ${distanceMatrix.length}x${distanceMatrix[0]?.length}, normalizedEmployees: ${normalizedEmployees.length}`);
    console.log(`[DEBUG-MATRIX] Sample matrix[0][1]=${distanceMatrix[0]?.[1]}, matrix[1][0]=${distanceMatrix[1]?.[0]}`);

    // Start with existing employees in their original order
    let currentRoute = existingEmployees.map(emp => ({
      ...emp,
      matrixIndex: empToMatrixIndex.get(emp.empCode)
    }));

    console.log(`[DEBUG-ROUTE] existingEmployees empCodes: ${existingEmployees.map(e => e.empCode).join(', ')}`);
    console.log(`[DEBUG-ROUTE] currentRoute matrixIndices: ${currentRoute.map(e => e.matrixIndex).join(', ')}`);

    // For each new employee, find the best insertion position
    for (const newEmp of newEmployees) {
      const newEmpMatrixIndex = empToMatrixIndex.get(newEmp.empCode);
      console.log(`[DEBUG-NEW] newEmp ${newEmp.empCode} matrixIndex=${newEmpMatrixIndex}, geoY=${newEmp.geoY}, geoX=${newEmp.geoX}`);

      if (newEmpMatrixIndex === undefined) {
        console.warn(`[RE-OPTIMIZE] Could not find matrix index for ${newEmp.empCode}`);
        continue;
      }

      let bestPosition = -1;
      let bestCost = Infinity;

      // Calculate cost of inserting at each position
      // Route structure: [Facility] -> Emp0 -> Emp1 -> ... -> EmpN -> [Facility] (for PICKUP it's reversed)

      for (let insertPos = 0; insertPos <= currentRoute.length; insertPos++) {
        let insertionCost = 0;

        // For both PICKUP and DROPOFF, we model the route as:
        // PICKUP:  [START] -> Emp[0] -> Emp[1] -> ... -> Emp[N] -> Facility
        // DROPOFF: Facility -> Emp[0] -> Emp[1] -> ... -> Emp[N] -> [END]
        // 
        // We use facility (index 0) as the reference point for both start and end
        // Insertion cost = (new arcs added) - (arc removed)

        if (currentRoute.length === 0) {
          // Empty route, cost is just distance to/from facility
          insertionCost = isPickup
            ? distanceMatrix[newEmpMatrixIndex][0]  // newEmp -> Facility
            : distanceMatrix[0][newEmpMatrixIndex]; // Facility -> newEmp
        } else if (insertPos === 0) {
          // Inserting at the beginning
          const firstEmpIndex = currentRoute[0].matrixIndex;
          const firstDistFromFacility = distanceMatrix[0][firstEmpIndex];
          const newDistFromFacility = distanceMatrix[0][newEmpMatrixIndex];

          if (isPickup) {
            // PICKUP: Modeling as round trip: Facility -> First -> ... -> Last -> Facility
            // Inserting NewEmp before First: Facility -> NewEmp -> First -> ...
            // Cost = dist(Facility->NewEmp) + dist(NewEmp->First) - dist(Facility->First)
            const toNew = distanceMatrix[0][newEmpMatrixIndex];
            const newToFirst = distanceMatrix[newEmpMatrixIndex][firstEmpIndex];
            const toFirst = distanceMatrix[0][firstEmpIndex];
            insertionCost = toNew + newToFirst - toFirst;

            console.log(`[DEBUG-POS0] PICKUP: toNew=${toNew}, newToFirst=${newToFirst}, toFirst=${toFirst}, baseCost=${insertionCost}, newDist=${newDistFromFacility}, firstDist=${firstDistFromFacility}`);

            // If NewEmp is closer to facility than First, add heavy penalty (wrong direction for PICKUP)
            // PICKUP should start from farthest employee, so position 0 should have the farthest
            if (newDistFromFacility < firstDistFromFacility) {
              insertionCost += (firstDistFromFacility - newDistFromFacility) * 10;
              console.log(`[DEBUG-POS0] Added penalty: ${(firstDistFromFacility - newDistFromFacility) * 10}, totalCost=${insertionCost}`);
            }
          } else {
            // DROPOFF: Facility -> First -> ... 
            // Inserting NewEmp: Facility -> NewEmp -> First -> ...
            // Cost = dist(Facility->NewEmp) + dist(NewEmp->First) - dist(Facility->First)
            insertionCost = distanceMatrix[0][newEmpMatrixIndex]
              + distanceMatrix[newEmpMatrixIndex][firstEmpIndex]
              - distanceMatrix[0][firstEmpIndex];

            // If NewEmp is farther from facility than First, add penalty (wrong direction for DROPOFF)
            // DROPOFF should start with nearest employee first
            if (newDistFromFacility > firstDistFromFacility) {
              insertionCost += (newDistFromFacility - firstDistFromFacility) * 10;
            }
          }
        } else if (insertPos === currentRoute.length) {
          // Inserting at the end
          const lastEmpIndex = currentRoute[currentRoute.length - 1].matrixIndex;
          if (isPickup) {
            // PICKUP: ... -> Last -> Facility
            // Inserting NewEmp: ... -> Last -> NewEmp -> Facility
            // Cost = dist(Last->NewEmp) + dist(NewEmp->Facility) - dist(Last->Facility)
            insertionCost = distanceMatrix[lastEmpIndex][newEmpMatrixIndex]
              + distanceMatrix[newEmpMatrixIndex][0]
              - distanceMatrix[lastEmpIndex][0];

            // If NewEmp is farther from facility than Last, add penalty (wrong direction)
            const lastDistFromFacility = distanceMatrix[0][lastEmpIndex];
            const newDistFromFacility = distanceMatrix[0][newEmpMatrixIndex];
            if (newDistFromFacility > lastDistFromFacility) {
              insertionCost += (newDistFromFacility - lastDistFromFacility) * 10;
            }
          } else {
            // DROPOFF: ... -> Last -> [END]
            // Inserting NewEmp: ... -> Last -> NewEmp
            const lastDistFromFacility = distanceMatrix[0][lastEmpIndex];
            const newDistFromFacility = distanceMatrix[0][newEmpMatrixIndex];

            insertionCost = distanceMatrix[lastEmpIndex][newEmpMatrixIndex];

            // If NewEmp is closer to facility than Last, add penalty (wrong direction)
            if (newDistFromFacility < lastDistFromFacility) {
              insertionCost += (lastDistFromFacility - newDistFromFacility) * 10;
            }
          }
        } else {
          // Inserting in the middle (same for both PICKUP and DROPOFF)
          const prevEmpIndex = currentRoute[insertPos - 1].matrixIndex;
          const nextEmpIndex = currentRoute[insertPos].matrixIndex;

          // Classic cheapest insertion: Cost = dist(prev->new) + dist(new->next) - dist(prev->next)
          insertionCost = distanceMatrix[prevEmpIndex][newEmpMatrixIndex]
            + distanceMatrix[newEmpMatrixIndex][nextEmpIndex]
            - distanceMatrix[prevEmpIndex][nextEmpIndex];
        }

        // Add direction penalty for middle positions
        if (insertPos > 0 && insertPos < currentRoute.length) {
          const prevDist = distanceMatrix[0][currentRoute[insertPos - 1].matrixIndex];
          const newDist = distanceMatrix[0][newEmpMatrixIndex];
          const nextDist = distanceMatrix[0][currentRoute[insertPos].matrixIndex];

          if (isPickup) {
            // For PICKUP, distance should decrease: prevDist >= newDist >= nextDist
            if (newDist > prevDist) insertionCost += (newDist - prevDist) * 5;
            if (newDist < nextDist) insertionCost += (nextDist - newDist) * 5;
          } else {
            // For DROPOFF, distance should increase: prevDist <= newDist <= nextDist
            if (newDist < prevDist) insertionCost += (prevDist - newDist) * 5;
            if (newDist > nextDist) insertionCost += (newDist - nextDist) * 5;
          }
        }

        if (insertionCost < bestCost) {
          bestCost = insertionCost;
          bestPosition = insertPos;
        }
      } // end of for (insertPos) loop

      // Insert the new employee at the best position
      if (bestPosition >= 0) {
        currentRoute.splice(bestPosition, 0, {
          ...newEmp,
          matrixIndex: newEmpMatrixIndex
        });
        console.log(`[RE-OPTIMIZE] Inserted ${newEmp.empCode} at position ${bestPosition} (cost: ${bestCost.toFixed(0)}m)`);
      }
    }

    // Build final optimized employee list (remove matrixIndex)
    const optimizedEmployees = currentRoute.map(({ matrixIndex, ...emp }) => emp);

    console.log(`[RE-OPTIMIZE] Insertion complete. Final order: ${optimizedEmployees.map(e => e.empCode).join(' -> ')}`);

    // Calculate route details for the optimized order
    const optimizedEmployeeCoords = optimizedEmployees.map(emp => [emp.geoY, emp.geoX]);
    const routeCoords = isPickup
      ? [...optimizedEmployeeCoords, [facility.geoY, facility.geoX]]
      : [[facility.geoY, facility.geoX], ...optimizedEmployeeCoords];

    const routeDetails = await calculateRouteDetails(
      routeCoords,
      optimizedEmployees,
      pickupTimePerEmployee,
      tripType,
      city,
      shiftTime
    );

    return {
      reOptimized: true,
      employees: routeDetails.employees || optimizedEmployees,
      routeDetails
    };

  } catch (error) {
    console.error("[RE-OPTIMIZE] Error:", error);
    return {
      reOptimized: false,
      employees,
      error: error.message
    };
  }
}

/**
 * Full VRP-based reoptimization (used when no existing order to preserve)
 * @param {Object} params - Configuration parameters
 * @returns {Object} - Optimized employees with route details
 */
async function reoptimizeRouteWithFullVRP({
  employees,
  facility,
  tripType,
  vehicleCapacity,
  pickupTimePerEmployee,
  city,
  shiftTime,
  maxRouteDuration = 7200
}) {
  const facilityLocation = { lat: facility.geoY, lng: facility.geoX };
  const effectiveCapacity = vehicleCapacity || employees.length;
  const isPickup = tripType.toUpperCase() === 'PICKUP';

  // Normalize employees to have location object
  const normalizedEmployees = employees.map(emp => ({
    ...emp,
    location: emp.location || { lat: emp.geoY, lng: emp.geoX }
  }));

  // Generate distance/duration matrix using OSRM
  const matrixData = await generateDistanceDurationMatrix(
    normalizedEmployees,
    facilityLocation,
    city
  );

  const { distanceMatrix, durationMatrix, pointMap } = matrixData;

  if (!distanceMatrix || distanceMatrix.length === 0) {
    return {
      reOptimized: false,
      employees,
      error: "Failed to generate distance matrix"
    };
  }

  // Find the farthest employee for anchor pinning
  let maxDist = -1;
  let farthestEmployeeIndex = -1;

  for (let i = 1; i < distanceMatrix[0].length; i++) {
    const dist = distanceMatrix[0][i];
    if (dist > maxDist) {
      maxDist = dist;
      farthestEmployeeIndex = i;
    }
  }

  const fixedNodeParam = {};
  if (farthestEmployeeIndex !== -1) {
    if (isPickup) {
      fixedNodeParam.fixed_start_node_index_in_matrix = farthestEmployeeIndex;
    } else {
      fixedNodeParam.fixed_end_node_index_in_matrix = farthestEmployeeIndex;
    }
  }

  // Prepare OR-Tools input
  const demands = [0, ...normalizedEmployees.map(() => 1)];
  const serviceTimes = [0, ...normalizedEmployees.map(() => pickupTimePerEmployee)];

  const orToolsInput = {
    distance_matrix: distanceMatrix,
    duration_matrix: durationMatrix,
    num_vehicles: 1,
    vehicle_capacities: [effectiveCapacity],
    demands: demands,
    depot_index: 0,
    max_route_duration: maxRouteDuration,
    service_times: serviceTimes,
    allow_dropping_visits: false,
    facility_coords: [facilityLocation.lat, facilityLocation.lng],
    trip_type: tripType.toUpperCase(),
    direction_penalty_weight: 5.0,
    ...fixedNodeParam
  };

  const pythonExecutable = process.env.PYTHON_EXECUTABLE || "python";
  const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py");
  if (!fs.existsSync(scriptPath)) {
    throw new Error(`Solver script not found: ${scriptPath}`);
  }

  // Spawn Python VRP solver
  const pythonProcess = spawn(pythonExecutable, [scriptPath]);
  let scriptOutput = "";
  let scriptError = "";

  pythonProcess.stdin.write(JSON.stringify(orToolsInput));
  pythonProcess.stdin.end();

  pythonProcess.stdout.on("data", (data) => (scriptOutput += data.toString()));
  pythonProcess.stderr.on("data", (data) => (scriptError += data.toString()));

  const solverResult = await new Promise((resolve) => {
    pythonProcess.on("close", (code) => {
      if (code !== 0) {
        console.error(`[RE-OPTIMIZE] Python solver exit ${code}: ${scriptError}`);
        return resolve({ error: `Solver failed with code ${code}` });
      }
      try {
        let solution = null;
        const lines = scriptOutput.trim().split("\n");
        for (let i = lines.length - 1; i >= 0; i--) {
          try {
            solution = JSON.parse(lines[i]);
            if (typeof solution === "object" && solution !== null) break;
          } catch (e) { /* skip non-JSON lines */ }
        }
        if (!solution) return resolve({ error: "No valid JSON from solver" });
        if (solution.error) return resolve({ error: solution.error });
        resolve(solution);
      } catch (e) {
        resolve({ error: `Parse error: ${e.message}` });
      }
    });
    pythonProcess.on("error", (err) => resolve({ error: `Spawn error: ${err.message}` }));
  });

  if (solverResult.error) {
    console.warn(`[RE-OPTIMIZE] VRP solver failed: ${solverResult.error}. Keeping original order.`);
    return {
      reOptimized: false,
      employees,
      error: solverResult.error
    };
  }

  if (!solverResult.routes || solverResult.routes.length === 0) {
    return {
      reOptimized: false,
      employees,
      error: "No routes returned by solver"
    };
  }

  // Extract optimized employee order from solver result
  const routeNodeIndices = solverResult.routes[0].node_indices;
  const optimizedEmployees = routeNodeIndices
    .map(nodeIndex => {
      if (nodeIndex === 0 || nodeIndex >= pointMap.length) return null;
      const mapped = pointMap[nodeIndex];
      return mapped.isFacility ? null : mapped;
    })
    .filter(emp => emp !== null);

  // Calculate route details for the optimized order
  const optimizedEmployeeCoords = optimizedEmployees.map(emp => [emp.geoY, emp.geoX]);
  const routeCoords = isPickup
    ? [...optimizedEmployeeCoords, [facility.geoY, facility.geoX]]
    : [[facility.geoY, facility.geoX], ...optimizedEmployeeCoords];

  const routeDetails = await calculateRouteDetails(
    routeCoords,
    optimizedEmployees,
    pickupTimePerEmployee,
    tripType,
    city,
    shiftTime
  );

  return {
    reOptimized: true,
    employees: routeDetails.employees || optimizedEmployees,
    routeDetails
  };
}

module.exports = {
  generateRoutes,
  isOsrmAvailable,
  calculateRouteDetails,
  calculatePickupTimes,
  getFastApiCityKey,
  OSRM_PROBE_TIMEOUT,
  calculateFarthestEmployeeDistance,
  calculateEta,
  reoptimizeRouteWithVRP,
  generateDistanceDurationMatrix
};
