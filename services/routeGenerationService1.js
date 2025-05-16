const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const { spawn } = require("child_process"); // For calling Python

const TRAFFIC_BUFFER_PERCENTAGE = 0.4; // 40% buffer for traffic
const MAX_SWAP_DISTANCE_KM = 4; // or your business threshold

// At the top with other constants, or inside generateRoutes/processEmployeeBatch
const OSRM_PROBE_COUNT = 10; // Number of top candidates to probe with OSRM
const OSRM_PROBE_TIMEOUT = 3000; // Timeout for the probing OSRM call (milliseconds)

const fetchApi = (...args) => {
  return import("node-fetch").then(({ default: fetch }) => fetch(...args));
};

const ZONES_DATA_FILE = path.join(__dirname, "../data/delhi_ncr_zones.json");

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
  // point1 and point2 are [lat, lng]
  const [lat1, lng1] = point1;
  const [lat2, lng2] = point2;

  const R = 6371; // Earth's radius in kilometers

  const dLat = toRadians(lat2 - lat1);
  const dLng = toRadians(lng2 - lng1);

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLng / 2) *
      Math.sin(dLng / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  const distance = R * c; // in kilometers

  return distance;
}

function haversineDistance([lat1, lon1], [lat2, lon2]) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371; // km
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) *
      Math.cos(toRad(lat2)) *
      Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function isPointInPolygon(point, polygon) {
  if (!point || !polygon || !Array.isArray(polygon)) return false;
  const [lat, lng] = point;
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const [lngI, latI] = polygon[i]; // Note: GeoJSON often uses [lng, lat]
    const [lngJ, latJ] = polygon[j];
    const intersect =
      latI > lat !== latJ > lat &&
      lng < ((lngJ - lngI) * (lat - latI)) / (latJ - latI) + lngI;
    if (intersect) inside = !inside;
  }
  return inside;
}

function calculateAngle(point1, point2) {
  // Calculates angle in radians from point1 to point2 relative to positive x-axis
  return Math.atan2(point2.lat - point1.lat, point2.lng - point1.lng);
}

function angleDifference(angle1, angle2) {
  // Calculates the smallest difference between two angles (in radians)
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
    const zonePolygon = zone.geometry.coordinates[0]; // Assumes first polygon if MultiPolygon
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
    (emp) => !assignedEmployees.has(emp.empCode) && emp.geoX && emp.geoY,
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
  if (
    profile.LargeCapacityZones &&
    profile.LargeCapacityZones.includes(zoneName)
  )
    return 12;
  if (
    profile.MediumCapacityZones &&
    profile.MediumCapacityZones.includes(zoneName)
  )
    return 6;
  if (
    profile.SmallCapacityZones &&
    profile.SmallCapacityZones.includes(zoneName)
  )
    return 4;
  return 6; // default
}

// THIS IS THE FUNCTION THAT WAS UNDEFINED
async function calculateRouteDetails(
  routeCoordinates,
  employees,
  pickupTimePerEmployee, // This parameter is not used in this function but kept for signature consistency
  tripType = "pickup",
) {
  try {
    if (!routeCoordinates?.length || !employees?.length) {
      console.error("calculateRouteDetails: Invalid input parameters", { routeCoordinates, employees });
      throw new Error("Invalid input parameters for calculateRouteDetails");
    }

    const coordinatesString = routeCoordinates
      .map((coord) => `${coord[1]},${coord[0]}`) // OSRM expects lng,lat
      .join(";");

    const url = `http://localhost:5000/trip/v1/driving/${coordinatesString}?source=first&destination=last&roundtrip=false&steps=true&geometries=polyline&overview=full`;

    const response = await fetchApi(url);
    if (!response.ok) {
      console.error(`calculateRouteDetails: OSRM /trip service error: ${response.status} for URL: ${url}`);
      throw new Error(`OSRM /trip service error: ${response.status}`);
    }

    const data = await response.json();

    if (data.code !== "Ok" || !data.trips?.[0]) {
      console.error("calculateRouteDetails: Invalid OSRM /trip response or no trip found", data);
      throw new Error("Invalid OSRM /trip response or no trip found");
    }

    const trip = data.trips[0];
    const waypoints = data.waypoints; // OSRM waypoints

    // Build the full road-following polyline from the main trip geometry if available
    // or concatenate step geometries if detailed overview is not 'full' or missing.
    let fullRoadPolyline = trip.geometry ? trip.geometry : ""; // OSRM polyline5
    if (!fullRoadPolyline && trip.legs) {
        let fullCoords = [];
        for (const leg of trip.legs) {
            for (const step of leg.steps) {
                const coords = decodePolyline(step.geometry);
                if (fullCoords.length > 0 && coords.length > 0) { // Avoid popping if coords is empty
                    const lastPtFull = fullCoords[fullCoords.length -1];
                    const firstPtCoords = coords[0];
                    if(lastPtFull[0] === firstPtCoords[0] && lastPtFull[1] === firstPtCoords[1]){
                        fullCoords.pop();
                    }
                }
                fullCoords = fullCoords.concat(coords);
            }
        }
        fullRoadPolyline = encodePolyline(fullCoords);
    }


    let orderedEmployees;
    if (tripType.toLowerCase() === "pickup") {
      // Waypoints from OSRM are in the order of the input coordinates.
      // The first waypoint (index 0) is the first pickup, last before facility.
      // The last waypoint in OSRM's list corresponds to the facility.
      // We need to map OSRM waypoints (excluding the facility) back to original employees.
      // `employees` array is the original list of employees for this route, in their initial order.
      orderedEmployees = waypoints
        .slice(0, -1) // Exclude the last waypoint (facility)
        .sort((a, b) => a.trips_index - b.trips_index) // Sort by the order in the trip
        .map((wp, i) => {
            // wp.waypoint_index is the index in the *original input coordinate list* to OSRM
            // For pickup, employees are 0 to N-1, facility is N.
            return {
                ...employees[wp.waypoint_index],
                order: i + 1, // Sequential order in the optimized route
            };
        });
    } else { // Dropoff
      // First waypoint is facility, subsequent are employees.
      // `employees` array is the original list of employees for this route.
      orderedEmployees = waypoints
        .slice(1) // Exclude the first waypoint (facility)
        .sort((a, b) => a.trips_index - b.trips_index) // Sort by the order in the trip
        .map((wp, i) => {
            // wp.waypoint_index is the index in the *original input coordinate list* to OSRM
            // For dropoff, facility is 0, employees are 1 to N.
            // So, waypoint_index 1 corresponds to employees[0], etc.
            return {
                ...employees[wp.waypoint_index - 1],
                order: i + 1,
            };
        });
    }

    const legs = trip.legs || [];

    return {
      employees: orderedEmployees,
      totalDistance: trip.distance, // in meters
      totalDuration: trip.duration * (1 + TRAFFIC_BUFFER_PERCENTAGE), // in seconds
      encodedPolyline: fullRoadPolyline,
      legs: legs, // OSRM legs for detailed turn-by-turn (optional)
      geometry: { // GeoJSON LineString
        type: "LineString",
        coordinates: decodePolyline(fullRoadPolyline).map(coord => [coord[1], coord[0]]), // lng, lat
      },
    };
  } catch (error) {
    console.error("calculateRouteDetails: Critical error during calculation:", error);
    // Fallback: return employees in original order with zero distance/duration
    return {
      employees: employees.map((emp, index) => ({
        ...emp,
        order: index + 1, // Keep original order on error
      })),
      totalDistance: 0,
      totalDuration: 0,
      encodedPolyline: "",
      legs: [],
      geometry: null,
      error: error.message || "Failed to calculate route details",
    };
  }
}


function groupEmployeesByAngle(employees, facility, numGroups = 4) {
  const employeesWithAngles = employees.map((emp) => {
    const angle =
      (Math.atan2(
        emp.location.lat - facility.geoY,
        emp.location.lng - facility.geoX,
      ) *
        180) /
      Math.PI;
    return { ...emp, angle: (angle + 360) % 360 };
  });

  employeesWithAngles.sort((a, b) => a.angle - b.angle);

  const groups = [];
  const groupSize = Math.ceil(employeesWithAngles.length / numGroups);

  for (let i = 0; i < employeesWithAngles.length; i += groupSize) {
    groups.push(employeesWithAngles.slice(i, i + groupSize));
  }

  return groups;
}


// NEW FUNCTION: To generate OSRM matrix for OR-Tools
async function generateDistanceDurationMatrix(locationsForMatrix, facilityLocation) {
  // locationsForMatrix: array of employee objects { location: {lat, lng}, empCode: ... }
  // facilityLocation: { lat, lng }

  const allPointsCoords = [
    facilityLocation, // Depot is index 0
    ...locationsForMatrix.map(emp => emp.location)
  ];

  if (allPointsCoords.length <= 1) {
    console.warn("[MatrixGen] Not enough points for a matrix:", allPointsCoords.length);
    return { distanceMatrix: [[]], durationMatrix: [[]], pointMap: [] };
  }

  const coordinatesString = allPointsCoords
    .map(p => `${p.lng},${p.lat}`) // OSRM expects lng,lat
    .join(";");

  // Increase timeout based on number of points (N*N requests essentially)
  const matrixTimeout = OSRM_PROBE_TIMEOUT + (allPointsCoords.length * 200); // Base + per point
  const osrmTableUrl = `http://localhost:5000/table/v1/driving/${coordinatesString}?annotations=duration,distance`;
  console.log(`[MatrixGen] OSRM Table URL for matrix (${allPointsCoords.length} points): ${osrmTableUrl.substring(0,150)}...`);

  try {
    const response = await fetchApi(osrmTableUrl, { timeout: matrixTimeout });
    if (!response.ok) {
      const errorText = await response.text(); // Try to get error body
      console.error(`[MatrixGen] OSRM /table HTTP error: ${response.status}. Body: ${errorText}`);
      throw new Error(`OSRM table service error for matrix: ${response.status}`);
    }
    const data = await response.json();
    console.log("[MatrixGen] Raw OSRM table data:", JSON.stringify(data).substring(0, 500) + "..."); // Log snippet

    if (data.code !== "Ok" || !data.durations || !data.distances) {
      console.error("[MatrixGen] Invalid OSRM table response structure or error code:", data);
      throw new Error("Invalid OSRM table response for matrix (structure or code)");
    }
    // OSRM returns durations in seconds, distances in meters
    // pointMap helps map solver indices (0 for depot, 1 to N for employees) back to original employee data
    const pointMap = [
        { empCode: 'FACILITY', isFacility: true, ...facilityLocation }, // Depot info
        ...locationsForMatrix // Original employee objects
    ];
    return {
        distanceMatrix: data.distances,
        durationMatrix: data.durations,
        pointMap: pointMap
    };
  } catch (error) {
    console.error("[MatrixGen] Failed to generate distance/duration matrix:", error);
    throw error;
  }
}


// NEW FUNCTION: Replaces processEmployeeBatch for OR-Tools
// Node.js: routeGenerationService.js

// ... (ensure all other necessary imports like fs, path, spawn, and helper functions are above this)

async function solveZoneWithORTools(
  zoneEmployees,       // Array of employee objects for this zone
  facilityData,        // Facility object { geoX, geoY, ... }
  vehicleCapacity,     // Single capacity value for vehicles in this zone
  maxRouteDurationSeconds, // Max duration for any route in this zone
  pickupTimePerEmployee, // Service time per employee
  tripType,            // "pickup" or "dropoff"
  zoneName             // <--- THIS IS THE NEW PARAMETER
) {
  if (!zoneEmployees || zoneEmployees.length === 0) {
    return { routes: [] };
  }

  // Use the passed zoneName for logging. If not passed, try to get from first employee, else default.
  const currentZoneNameForLogging = zoneName || (zoneEmployees[0]?.zone) || "UNKNOWN_ZONE_IN_SOLVER";

  console.log(`\n[OR-TOOLS SOLVER] Solving for zone: "${currentZoneNameForLogging}" with ${zoneEmployees.length} employees. VehCap: ${vehicleCapacity}, MaxRouteDur: ${maxRouteDurationSeconds}s, PickupTime: ${pickupTimePerEmployee}s. Assuming ample vehicles.`);

  const facilityLocation = { lat: facilityData.geoY, lng: facilityData.geoX };
  let pointMapForCurrentZone = [];

  try {
    const matrixData = await generateDistanceDurationMatrix(zoneEmployees, facilityLocation);
    const { distanceMatrix, durationMatrix } = matrixData;
    pointMapForCurrentZone = matrixData.pointMap;

    if (!distanceMatrix || distanceMatrix.length === 0 || (distanceMatrix.length > 0 && distanceMatrix[0].length === 0)) {
        console.warn(`[OR-TOOLS SOLVER] Empty or invalid distance matrix for zone "${currentZoneNameForLogging}", cannot solve.`);
        return { routes: [] };
    }
     if (pointMapForCurrentZone.length !== distanceMatrix.length) {
        console.error(`[OR-TOOLS SOLVER] Mismatch between pointMap length (${pointMapForCurrentZone.length}) and matrix dimensions (${distanceMatrix.length}) for zone "${currentZoneNameForLogging}"!`);
        return { routes: [] };
    }

    const numCustomers = zoneEmployees.length;
    const numVehiclesForSolver = numCustomers > 0 ? numCustomers : 1; 

    console.log(`[OR-TOOLS SOLVER] Using numVehiclesForSolver: ${numVehiclesForSolver} for zone "${currentZoneNameForLogging}"`);

    const demands = [0]; 
    zoneEmployees.forEach(() => demands.push(1)); 

    const serviceTimes = [0]; 
    zoneEmployees.forEach(() => serviceTimes.push(pickupTimePerEmployee));

    const orToolsInput = {
      distance_matrix: distanceMatrix,
      duration_matrix: durationMatrix,
      num_vehicles: numVehiclesForSolver,
      vehicle_capacities: Array(numVehiclesForSolver).fill(vehicleCapacity),
      demands: demands,
      depot_index: 0, 
      max_route_duration: maxRouteDurationSeconds,
      service_times: serviceTimes,
      // Example: control dropping visits via a profile setting if you implement it
      // allow_dropping_visits: facilityData.profile?.allowDroppingVisitsForProblematicZones || false,
      // drop_visit_penalty: facilityData.profile?.dropPenalty || 5000000
    };
    
    // --- DEBUG LOGGING CODE USING currentZoneNameForLogging ---
    // Define a list of zones you want to debug specifically
    // const zonesToDebug = ["MOHAN_GARDEN", "DEFAULT_ZONE", "PALAM_VIHAR", "SOHNA_ROAD", "DILSHAD_GARDEN", "YAMUNA_VIHAR", "SHAHDARA", "UTTAM_NAGAR"];
    // if (zonesToDebug.includes(currentZoneNameForLogging)) {
    //     const safeZoneName = currentZoneNameForLogging.replace(/[^a-zA-Z0-9_.-]/g, '_');
    //     const debugFileName = `debug_input_${safeZoneName}_${Date.now()}.json`;
    //     try {
    //         // Ensure the path is correct, e.g., save to a specific debug directory
    //         const debugDirPath = path.join(__dirname, '../debug_solver_inputs'); // Example: ../debug_solver_inputs
    //         if (!fs.existsSync(debugDirPath)){
    //             fs.mkdirSync(debugDirPath, { recursive: true });
    //         }
    //         fs.writeFileSync(path.join(debugDirPath, debugFileName), JSON.stringify(orToolsInput, null, 2));
    //         console.log(`[OR-TOOLS SOLVER] Saved input for ${currentZoneNameForLogging} to ${path.join(debugDirPath, debugFileName)}`);
    //     } catch (writeErr) {
    //         console.error(`[OR-TOOLS SOLVER] Error saving debug input for ${currentZoneNameForLogging}:`, writeErr);
    //     }
    // }
    // --- END OF DEBUG LOGGING CODE ---
        
    const pythonExecutable = "python"; 
    const scriptPath = path.join(__dirname, "or_tools_vrp_solver.py"); // Assuming it's in the same directory as this service file

    if (!fs.existsSync(scriptPath)) {
        console.error(`[OR-TOOLS SOLVER] Python solver script not found at: ${scriptPath}`);
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
      console.error(`[OR-TOOLS Python stderr FOR ZONE ${currentZoneNameForLogging}]: ${errData}`);
      scriptError += errData;
    });

    return new Promise((resolve, reject) => {
      pythonProcess.on("close", (code) => {
        let solution = null;
        let parsedSuccessfully = false;
        let parseErrorDetail = null;

        if (scriptOutput && scriptOutput.trim() !== "") {
            try {
                let lastBraceIndex = scriptOutput.lastIndexOf('}');
                if (lastBraceIndex !== -1) {
                    let openBraceCount = 0;
                    let firstBraceIndex = -1;
                    for (let i = lastBraceIndex; i >= 0; i--) {
                        if (scriptOutput[i] === '}') openBraceCount++;
                        else if (scriptOutput[i] === '{') {
                            openBraceCount--;
                            if (openBraceCount === 0) {
                                firstBraceIndex = i;
                                break;
                            }
                        }
                    }
                    if (firstBraceIndex !== -1) {
                        const potentialJsonString = scriptOutput.substring(firstBraceIndex, lastBraceIndex + 1);
                        solution = JSON.parse(potentialJsonString);
                        parsedSuccessfully = true;
                    }
                }
                if (!parsedSuccessfully) {
                    console.warn(`[OR-TOOLS SOLVER] Robust JSON extraction failed for zone "${currentZoneNameForLogging}", trying to parse last line if it's JSON.`);
                    const lines = scriptOutput.trim().split('\n');
                    const lastLine = lines[lines.length - 1].trim();
                    if (lastLine.startsWith('{') && lastLine.endsWith('}')) {
                        solution = JSON.parse(lastLine);
                        parsedSuccessfully = true;
                    } else {
                         throw new Error("Last line is not valid JSON.");
                    }
                }
            } catch (e) {
                parseErrorDetail = e.message;
                console.error(`[OR-TOOLS SOLVER] Error parsing Python script stdout for zone "${currentZoneNameForLogging}":`, e, "\nRaw stdout was:\n", scriptOutput);
            }
        }

        if (code !== 0) {
            let pythonErrorMessage = `Python script for zone "${currentZoneNameForLogging}" exited with code ${code}.`;
            if (scriptError) {
                try {
                    const pyErrorObj = JSON.parse(scriptError);
                    if (pyErrorObj.error) {
                        pythonErrorMessage = `Python script error (zone "${currentZoneNameForLogging}"): ${pyErrorObj.error} - ${pyErrorObj.details || ''}`;
                    } else {
                         pythonErrorMessage += ` Stderr: ${scriptError}`;
                    }
                } catch (e) {
                    pythonErrorMessage += ` Stderr: ${scriptError}`;
                }
            }
            return reject(new Error(pythonErrorMessage));
        }
        
        if (!parsedSuccessfully || !solution) {
            return reject(new Error(`Failed to parse OR-Tools solution for zone "${currentZoneNameForLogging}". Parse error: ${parseErrorDetail || 'Unknown'}. Raw output: ${scriptOutput.substring(0,500)}`));
        }

        if (solution.error) {
            console.error(`[OR-TOOLS SOLVER] Python script's JSON response for zone "${currentZoneNameForLogging}" indicated an error:`, solution.error, solution.details || '');
            return reject(new Error(`OR-Tools solver error for zone "${currentZoneNameForLogging}": ${solution.error}`));
        }

        const orRoutes = [];
        if (solution.routes && Array.isArray(solution.routes)) {
            solution.routes.forEach((routeNodeIndices) => {
              if (routeNodeIndices.length > 0) {
                const currentRouteEmployees = routeNodeIndices.map(
                  (nodeIndex) => {
                    if (nodeIndex === 0) return null; 
                    if (nodeIndex >= pointMapForCurrentZone.length) {
                        console.error(`[OR-TOOLS SOLVER] Error for zone "${currentZoneNameForLogging}": nodeIndex ${nodeIndex} is out of bounds for pointMapForCurrentZone (length ${pointMapForCurrentZone.length})`);
                        return null;
                    }
                    return pointMapForCurrentZone[nodeIndex]; 
                  }
                ).filter(emp => emp != null && !emp.isFacility);

                if (currentRouteEmployees.length > 0) {
                  orRoutes.push({
                    employees: currentRouteEmployees,
                    vehicleCapacity: vehicleCapacity,
                    guardNeeded: false,
                    zone: currentZoneNameForLogging, // Use the passed zoneName
                    tripType: tripType,
                  });
                }
              }
            });
        }
        console.log(`[OR-TOOLS SOLVER] Successfully processed ${orRoutes.length} routes from solver for zone: "${currentZoneNameForLogging}".`);
        resolve({ routes: orRoutes });
      });

      pythonProcess.on('error', (err) => {
        console.error(`[OR-TOOLS SOLVER] Failed to start Python subprocess for zone "${currentZoneNameForLogging}".`, err);
        reject(err);
      });
    });
  } catch (error) {
    console.error(`[OR-TOOLS SOLVER] Critical error in solveZoneWithORTools for zone "${currentZoneNameForLogging}":`, error);
    const fallbackRoutes = zoneEmployees.map(emp => ({
        employees: [emp], vehicleCapacity: vehicleCapacity, guardNeeded: false,
        zone: currentZoneNameForLogging, tripType: tripType,
        error: `OR-Tools fallback: ${error.message}`
    }));
    return { routes: fallbackRoutes };
  }
}



// --- MODIFIED generateRoutes function ---
async function generateRoutes(data) {
  try {
    const {
      employees,
      facility,
      shiftTime,
      date,
      profile,
      saveToDatabase = false,
      pickupTimePerEmployee = 0, // Default if not provided
      reportingTime = 0,
      guard = false,
      tripType = "PICKUP",
    } = data;

    if (!employees?.length) throw new Error("Employee data is required");
    if (!facility?.geoX || !facility?.geoY)
      throw new Error("Valid facility data required");
    if (!date || !shiftTime || !profile)
      throw new Error("Missing required parameters: date, shiftTime, or profile");

    const osrmAvailable = await isOsrmAvailable();
    if (!osrmAvailable) throw new Error("OSRM routing service unavailable");

    const useZones =
      profile.zoneBasedRouting !== undefined
        ? !!profile.zoneBasedRouting
        : true;

    let employeesByZone = {};
    if (useZones) {
      let zones = data.zones || [];
      if (!zones.length && ZONES_DATA_FILE) {
        try {
          zones = await loadZonesData();
          if (!zones.length) console.warn("No zones data loaded from file, but zone routing is enabled.");
        } catch (err) {
          console.error(`Failed to load zones: ${err.message}. Proceeding without file-based zones.`);
        }
      }
      employeesByZone = assignEmployeesToZones(employees, zones);
      if (Object.keys(employeesByZone).length === 0 && employees.length > 0) {
        console.warn("No employees assigned to any specific zones, all might go to DEFAULT_ZONE.");
         // Ensure DEFAULT_ZONE exists if all employees are unassigned
        if (!employeesByZone["DEFAULT_ZONE"] && employees.length > 0) {
            employeesByZone["DEFAULT_ZONE"] = employees.map(emp => ({
                ...emp,
                zone: "DEFAULT_ZONE",
                location: { lat: emp.geoY, lng: emp.geoX }
            }));
            console.log("Assigned all employees to DEFAULT_ZONE as no other zones matched.");
        }
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
    const {
      zonePairingMatrix = {},
      maxDuration: profileMaxDuration = 7200,
    } = profile;

    let totalRouteCount = 0;
    let finalTotalSwappedRoutes = 0;
    const allGeneratedRoutes = []; // Routes from OR-Tools (or fallback)
    const removedForGuardByZone = {};
    const isDropoff = tripType.toLowerCase() === "dropoff";

    // --- Zone Clubbing with OR-Tools ---
    if (profile.zoneClubbing) {
      const zoneGroups = findZoneGroups(profile.zonePairingMatrix || {});
      for (const group of zoneGroups) {
        const combinedEmployees = group.flatMap(
          (zoneName) => employeesByZone[zoneName] || [],
        ).filter(emp => emp.location); // Ensure employees have location

        if (combinedEmployees.length === 0) continue;
        console.log(`Processing clubbed zone group: ${group.join("-")} with ${combinedEmployees.length} employees.`);

        const maxCapacityForGroup = Math.max(
          ...group.map((z) => getZoneCapacity(z, profile)),1 // ensure at least 1
        );

        try {
          const { routes: batchRoutes } = await solveZoneWithORTools(
            combinedEmployees,
            facility,
            maxCapacityForGroup,
            profileMaxDuration,
            pickupTimePerEmployee,
            tripType
          );

          batchRoutes.forEach((route) => {
            route.zone = group.join("-");
            allGeneratedRoutes.push(route);
          });
          group.forEach((z) => processedZones.add(z));
        } catch (error) {
          console.error(`Error solving VRP for zone group ${group.join("-")}:`, error);
        }
      }
    }

    // --- Individual Zones with OR-Tools ---
    const remainingZoneEntries = Object.entries(employeesByZone).filter(
      ([zoneName]) => !processedZones.has(zoneName),
    );

    for (const [zoneName, zoneEmpList] of remainingZoneEntries) {
      const zoneEmployees = (zoneEmpList || []).filter(emp => emp.location); // Ensure employees have location
      if (zoneEmployees.length === 0) continue;
      console.log(`Processing individual zone: ${zoneName} with ${zoneEmployees.length} employees.`);

      const maxCapacityForZone = getZoneCapacity(zoneName, profile);
      try {
        const { routes: batchRoutes } = await solveZoneWithORTools(
          zoneEmployees,
          facility,
          maxCapacityForZone,
          profileMaxDuration,
          pickupTimePerEmployee,
          tripType
        );
        batchRoutes.forEach((route) => {
          route.zone = zoneName;
          allGeneratedRoutes.push(route);
        });
      } catch (error) {
        console.error(`Error solving VRP for zone ${zoneName}:`, error);
      }
    }

    // --- POST-PROCESSING LOOP (after allGeneratedRoutes are populated by OR-Tools) ---
    console.log(`\nStarting post-processing for ${allGeneratedRoutes.length} routes generated by OR-Tools/fallback.`);
    for (const route of allGeneratedRoutes) { // route object here comes from solveZoneWithORTools
      try {
        totalRouteCount++;
        route.routeNumber = totalRouteCount;
        let routeModifiedByGuardSwap = false;

        if (!route?.employees?.length) {
          console.warn(`Skipping route ${route.routeNumber} (Zone: ${route.zone}) - no employees after OR-Tools.`);
          assignErrorState(route, "No employees from OR-Tools solver");
          routeData.routeData.push(route);
          continue;
        }

        const routeCoordinates = route.employees
          .filter((emp) => emp.location?.lat && emp.location?.lng)
          .map((emp) => [emp.location.lat, emp.location.lng]);

        if (routeCoordinates.length === 0) {
          console.warn(`No valid coordinates for route ${route.routeNumber} (Zone: ${route.zone}).`);
          assignErrorState(route, "No valid coordinates in OR-Tools result");
          routeData.routeData.push(route);
          continue;
        }

        const facilityCoordinates = [facility.geoY, facility.geoX];
        const currentAllCoordinates = isDropoff
          ? [facilityCoordinates, ...routeCoordinates]
          : [...routeCoordinates, facilityCoordinates];

        let currentRouteDetails = await calculateRouteDetails(
          currentAllCoordinates,
          route.employees, // Employees are already ordered by OR-Tools
          pickupTimePerEmployee,
          tripType,
        );

        if (currentRouteDetails.error) {
          console.error(`OSRM /trip call failed for OR-Tools optimized route ${route.routeNumber}: ${currentRouteDetails.error}`);
          assignErrorState(route, `OSRM /trip failed: ${currentRouteDetails.error}`);
          routeData.routeData.push(route);
          continue;
        }
        updateRouteWithDetails(route, currentRouteDetails); // Updates route.employees with OSRM order (should match OR-Tools)

        // Guard Handling
        if (guard && route.employees.length > 0) {
          const guardSwapResult = await handleGuardRequirements(
            route, isDropoff, facility, pickupTimePerEmployee
          );

          if (guardSwapResult.swapped && guardSwapResult.routeDetails && !guardSwapResult.routeDetails.error) {
            routeModifiedByGuardSwap = true;
            finalTotalSwappedRoutes++;
            route.guardNeeded = false;
            updateRouteWithDetails(route, guardSwapResult.routeDetails);
          } else if (guardSwapResult.guardNeeded) {
            route.guardNeeded = true;
            let originalVehicleCapacity = route.vehicleCapacity; // Store before modification
            route.vehicleCapacity = Math.max(1, originalVehicleCapacity - 1);
            console.log(`Route ${route.routeNumber} needs guard. Capacity reduced from ${originalVehicleCapacity} to ${route.vehicleCapacity}.`);

            if (route.employees.length > route.vehicleCapacity) {
                const removedEmp = route.employees.pop(); // Simplistic: remove last employee
                if (removedEmp) {
                    console.log(`  Guard forced removal of ${removedEmp.empCode} from route ${route.routeNumber}.`);
                    if (!removedForGuardByZone[route.zone]) removedForGuardByZone[route.zone] = [];
                    removedForGuardByZone[route.zone].push(removedEmp);

                    // Recalculate route details for the shortened route
                    if (route.employees.length > 0) {
                        const newCoords = route.employees.map(e => [e.location.lat, e.location.lng]);
                        const newAllCoords = isDropoff ? [facilityCoordinates, ...newCoords] : [...newCoords, facilityCoordinates];
                        const recalcDetails = await calculateRouteDetails(newAllCoords, route.employees, pickupTimePerEmployee, tripType);
                        if (!recalcDetails.error) updateRouteWithDetails(route, recalcDetails);
                        else assignErrorState(route, `OSRM failed after guard removal: ${recalcDetails.error}`);
                    } else {
                         assignErrorState(route, "No employees after guard removal");
                    }
                }
            }
          }
        }

        if (route.employees.length > 0 && !route.error) {
          calculatePickupTimes(route, shiftTime, pickupTimePerEmployee, reportingTime);
          if (profileMaxDuration && route.routeDetails && route.routeDetails.duration > profileMaxDuration) {
            route.durationExceeded = true;
            console.warn(`Route ${route.routeNumber} exceeds max duration.`);
          }
        } else if (!route.error) {
            assignErrorState(route, "Route became empty or errored before time calculation");
        }

        route.swapped = routeModifiedByGuardSwap;
        routeData.routeData.push(route);

      } catch (error) {
        console.error(`Critical error in post-processing loop for route ${route?.routeNumber}:`, error);
        if (route) {
          assignErrorState(route, `Post-processing loop error: ${error.message}`);
          routeData.routeData.push(route);
        }
      }
    }

    // Re-insertion logic for employees removed due to guard (can remain largely the same)
    // ... (Your existing re-insertion logic) ...
    // Note: If creating new routes for these, ideally they'd also go through OR-Tools.
    // For simplicity now, let's assume your existing re-insertion creates small heuristic routes.

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
    const inputData = typeof data === 'object' && data !== null ? data : {};
    return createEmptyResponse({
        uuid: inputData.uuid, date: inputData.date, shiftTime: inputData.shiftTime,
        tripType: inputData.tripType, employees: inputData.employees,
    });
  }
}


function calculatePickupTimes(
  route,
  shiftTime,
  pickupTimePerEmployee,
  reportingTimeSeconds = 0,
) {
  try {
    if (!route?.employees?.length || !shiftTime) {
      throw new Error("Invalid input parameters for calculatePickupTimes");
    }

    const timeStr = shiftTime.toString().padStart(4, "0");
    const hours = parseInt(timeStr.substring(0, 2), 10);
    const minutes = parseInt(timeStr.substring(2, 4), 10);

    if (isNaN(hours) || isNaN(minutes)) {
      throw new Error("Invalid shift time format");
    }

    const facilityTargetTime = new Date();
    facilityTargetTime.setHours(hours, minutes, 0, 0);

    const isDropoff = route.tripType?.toLowerCase() === "dropoff";
    let currentTime = new Date(facilityTargetTime);

    if (!isDropoff) {
      route.facilityArrivalTime = formatTime(currentTime);
      for (let i = route.employees.length - 1; i >= 0; i--) {
        const employee = route.employees[i];
        const legDuration =
          (route.routeDetails?.legs?.[i]?.duration || 0) * // OSRM leg duration
          (1 + TRAFFIC_BUFFER_PERCENTAGE);
        currentTime.setSeconds(
          currentTime.getSeconds() - legDuration - pickupTimePerEmployee,
        );
        employee.pickupTime = formatTime(currentTime);
      }
    } else {
      route.facilityDepartureTime = formatTime(currentTime);
      for (let i = 0; i < route.employees.length; i++) {
        const employee = route.employees[i];
        const legDuration =
          (route.routeDetails?.legs?.[i]?.duration || 0) * // OSRM leg duration
          (1 + TRAFFIC_BUFFER_PERCENTAGE);
        currentTime.setSeconds(
          currentTime.getSeconds() + legDuration + pickupTimePerEmployee,
        );
        employee.dropoffTime = formatTime(currentTime);
        employee.pickupTime = employee.dropoffTime;
      }
    }
  } catch (error) {
    console.error("Time calculation error:", error);
    if(route && route.employees){
        route.employees.forEach((emp) => {
          emp.pickupTime = "Error";
          emp.dropoffTime = "Error";
        });
    }
    if(route){
        route.facilityArrivalTime = "Error";
        route.facilityDepartureTime = "Error";
    }
  }
}

function formatTime(date) {
  return date.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  });
}

async function handleGuardRequirements(
  route,
  isDropoff,
  facility,
  pickupTimePerEmployee,
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
        const distance = await calculateDistance( // Haversine for candidate search
          [criticalEmployee.location.lat, criticalEmployee.location.lng],
          [emp.location.lat, emp.location.lng],
        );
        return { employee: emp, index, distance };
      })
      .filter(Boolean);

    const validCandidates = (await Promise.all(swapCandidates))
      .filter(
        (candidate) =>
          candidate &&
          candidate.distance !== Infinity &&
          candidate.distance <= MAX_SWAP_DISTANCE_KM,
      )
      .sort((a, b) => a.distance - b.distance);

    if (validCandidates.length === 0) {
      console.log(
        `Guard needed for route ${route.routeNumber}: No suitable swap candidates found within ${MAX_SWAP_DISTANCE_KM}km.`,
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

    const routeDetailsAfterSwap = await calculateRouteDetails( // Recalculate with OSRM
      allCoordinates,
      newEmployees, // Pass the potentially swapped employee list
      pickupTimePerEmployee,
      route.tripType,
    );

    if (routeDetailsAfterSwap.error) {
      console.warn(
        `Swap validation failed for route ${route.routeNumber} due to OSRM error: ${routeDetailsAfterSwap.error}`,
      );
      return { guardNeeded: true, swapped: false }; // Revert or mark guard needed
    }

    // Update route with swapped employees and new details
    // route.employees = routeDetailsAfterSwap.employees; // calculateRouteDetails now returns ordered employees
    // updateRouteWithDetails(route, routeDetailsAfterSwap); // This will be done in the main loop

    console.log(
      `Successfully swapped employees in route ${route.routeNumber} for guard: ${criticalEmployee.empCode} with ${bestCandidate.employee.empCode}. Distance: ${bestCandidate.distance.toFixed(2)}km`,
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
      error,
    );
    return { guardNeeded: true, swapped: false };
  }
}

async function validateSwap(
  route,
  emp1Index,
  emp2Index,
  facility,
  pickupTimePerEmployee,
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
      route.tripType,
    );

    if (routeDetails.error) {
      return { viable: false, routeDetails }; // Return details even on error for logging
    }

    const originalDuration = route.routeDetails?.duration || Infinity; // Use current route's duration
    const newDuration = routeDetails.totalDuration;
    const durationIncrease = newDuration > originalDuration ? (newDuration - originalDuration) / originalDuration : 0;


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
  pickupTimePerEmployee,
) {
  // Placeholder for more complex swap logic (e.g., 3-way swaps, or considering multiple male candidates)
  return null;
}


function assignErrorState(route, message = "Unknown error") {
  if (!route) return;
  console.warn(`Assigning error state to route ${route.routeNumber || 'UNKNOWN'}: ${message}`);
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
      console.warn(`Not updating route ${route.routeNumber} with errored details: ${routeDetails.error}`);
      assignErrorState(route, `Failed to update with details: ${routeDetails.error}`);
      return;
  }
  route.employees = routeDetails.employees; // These are now OSRM ordered
  route.encodedPolyline = routeDetails.encodedPolyline;
  route.routeDetails = {
    distance: routeDetails.totalDistance,
    duration: routeDetails.totalDuration, // Already includes traffic buffer
    legs: routeDetails.legs,
    geometry: routeDetails.geometry
  };
  route.error = false; // Clear error if successfully updated
  route.errorMessage = undefined;
}

function calculateRouteStatistics(routeData, totalEmployeesInput) {
  const validRoutes = routeData.routeData.filter(route => !route.error && route.employees?.length > 0);
  const totalValidRoutes = validRoutes.length;
  const totalRoutedEmployees = validRoutes.reduce((sum, route) => sum + route.employees.length, 0);


  const averageOccupancy =
    totalValidRoutes > 0 ? totalRoutedEmployees / totalValidRoutes : 0;

  let totalDistanceSum = 0; // in meters
  let totalDurationSum = 0; // in seconds

  validRoutes.forEach((route) => {
    if (route.routeDetails?.duration !== Infinity && route.routeDetails?.distance !== Infinity) {
      totalDistanceSum += route.routeDetails?.distance || 0;
      totalDurationSum += route.routeDetails?.duration || 0; // This duration already includes traffic buffer
    }
  });

  return {
    totalEmployees: totalEmployeesInput,
    totalRoutedEmployees,
    totalRoutes: totalValidRoutes,
    averageOccupancy: parseFloat(averageOccupancy.toFixed(2)),
    routeDetails: { // Aggregated details
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
    tripType: routeData.tripType === "PICKUP" ? "P" : "D", // Standardize to P/D
    totalEmployees: routeData.totalEmployees,
    totalRoutedEmployees: routeData.totalRoutedEmployees,
    totalRoutes: routeData.totalRoutes,
    averageOccupancy: routeData.averageOccupancy,
    overallRouteDetails: routeData.routeDetails, // Renamed for clarity
    totalSwappedRoutes: routeData.totalSwappedRoutes,
    routes: routeData.routeData
        .filter(route => !route.error && route.employees?.length > 0)
        .map((route) => {
            const guardPresent = route.guardNeeded || (route.guard && route.employees.some(e => e.isGuard)); // Refined guard check
            const occupancy = (route.employees?.length || 0) + (guardPresent && !route.employees.some(e => e.isGuard) ? 1 : 0); // Don't double count if guard is an employee

            return {
                routeNumber: route.routeNumber,
                zone: route.zone,
                vehicleCapacity: route.vehicleCapacity,
                guard: guardPresent,
                swapped: route.swapped || false,
                durationExceeded: route.durationExceeded || false,
                uniqueKey: route.uniqueKey,
                distance: parseFloat(((route.routeDetails?.distance || 0) / 1000).toFixed(2)), // km
                duration: parseFloat((route.routeDetails?.duration || 0).toFixed(2)), // seconds
                occupancy,
                encodedPolyline: route.encodedPolyline || "no_polyline",
                employees: (route.employees || []).map((emp, index) => ({
                empCode: emp.empCode,
                gender: emp.gender,
                eta: route.tripType?.toUpperCase() === "DROPOFF" ? emp.dropoffTime : emp.pickupTime,
                order: emp.order !== undefined && emp.order >= 1 ? emp.order : index + 1,
                geoX: emp.geoX, // Assuming these are original geoX, geoY
                geoY: emp.geoY,
                // location: emp.location, // if needed for debugging
                })),
                // geometry: route.routeDetails?.geometry, // Optional: for map display
            };
    }),
  };
}

function createEmptyResponse(data) {
  return {
    uuid: data.uuid || uuidv4(),
    date: data.date,
    shift: data.shiftTime, // Ensure consistency with generateRoutes input
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
  // Export other functions if they need to be used externally or for testing
  // calculateRouteDetails, processEmployeeBatch (usually not exported if only internal)
};
