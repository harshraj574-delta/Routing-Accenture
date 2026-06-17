'use strict';

/**
 * Route Generation Service v3
 *
 * Architecture:
 *   1. Single OSRM /table call → full N×N distance + duration matrix
 *   2. OR-Tools multi-vehicle VRP on all employees at once (no zone walls)
 *      Direction penalty is baked into the cost matrix so OR-Tools naturally
 *      produces monotonic (non-zigzag) routes.
 *   3. Post-process: guard swap, OSRM /route per finalized route, pickup times,
 *      vehicle type assignment.
 *
 * Special employees (NMT / Medical / PWD) are routed separately with
 * per-type capacity constraints, then merged into the final response.
 */

const { v4: uuidv4 } = require('uuid');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// ─── constants ────────────────────────────────────────────────────────────────

const V3_BUILD = '2026-06-17-city-traffic-buffer';
console.log(`[v3] routeGenerationService_v3 loaded (build ${V3_BUILD})`);

const FASTAPI_GATEWAY_URL = 'https://mapapi.etmsonline.in';
const MAX_GUARD_SWAP_KM = 1.5;       // road distance threshold for guard swap
const SPECIAL_GROUP_MAX_GAP_KM = 5;  // max haversine gap between special employees in same group
const OSRM_TABLE_LIMIT = 100;        // max coordinates per single /table request (OSRM default)
const TABLE_CHUNK = 50;              // each src/dst chunk ≤ this (50+50=100 per call)

// ── unrouted-recovery tuning ──
// The recovery pass runs ONLY when the main pipeline dropped someone. These
// caps keep it fast (it touches just the few dropped employees, never the
// already-built routes' clustering/sequencing).
const RECOVERY_MAX_INSERT_GAP_KM = 4;    // emp must be within this of an existing route stop to attempt insertion
const RECOVERY_MAX_OSRM_TRIES    = 3;    // max OSRM route-verifications per emp during insertion (speed guard)
const RECOVERY_NEW_GROUP_MAX_GAP_KM = 3; // proximity chaining gap when forming new overflow routes

// ─── fetch helper ─────────────────────────────────────────────────────────────

// node-fetch v3 dropped the `timeout` option — it silently ignores it and the
// request can hang forever on a stalled connection. Translate it into an
// AbortSignal so every existing `{ timeout: N }` call site actually aborts.
const fetchApi = (url, opts = {}) => {
  const { timeout, ...rest } = opts;
  const fetchOpts = timeout ? { ...rest, signal: AbortSignal.timeout(timeout) } : rest;
  return import('node-fetch').then(({ default: f }) => f(url, fetchOpts));
};

// ─── utility functions ────────────────────────────────────────────────────────

function getFastApiCityKey(city) {
  const n = (city || '').toLowerCase();
  if (n === 'ncr' || n === 'delhi' || n === 'delhi ncr') return 'delhi';
  if (n === 'bengaluru' || n === 'bangalore') return 'bangalore';
  if (n === 'chennai') return 'chennai';
  return 'delhi';
}

// ── City-specific traffic model ──────────────────────────────────────────────
// Congestion is a per-city business rule: Delhi and Bengaluru peak at different
// hours and congest differently, so a single global buffer is wrong. Each city
// declares its high/moderate congestion WINDOWS (24h decimal — 08:30 = 8.5;
// hours outside them are LOW) and a duration BUFFER per level (the fraction
// added to OSRM travel time). Resolved via getFastApiCityKey() so it stays in
// lockstep with the road-network selection.
//
// PRODUCTION NOTE: this in-code table is the DEFAULT. The industry pattern (and
// the one this codebase already uses for routeDeviationRules) is to let the
// request `profile` carry an optional `trafficProfile` override that is merged
// over this default — so ops can retune windows/buffers per client without a
// deploy. getTrafficBuffer() reads the merged config.
const CITY_TRAFFIC = {
  bangalore: {                                 // Bengaluru
    high:     [[8.5, 13.0], [16.5, 22.0]],     // 08:30–13:00 & 16:30–22:00
    moderate: [[13.5, 16.0]],                  // 13:30–16:00
    buffers:  { low: 0.20, moderate: 0.30, high: 0.60 },
  },
  delhi: {                                     // Delhi / NCR
    high:     [[8.5, 12.5], [16.5, 21.0]],     // 08:30–12:30 & 16:30–21:00
    moderate: [[13.0, 16.0]],                  // 13:00–16:00
    buffers:  { low: 0.20, moderate: 0.30, high: 0.60 },
  },
};
const DEFAULT_TRAFFIC_BUFFERS = { low: 0.20, moderate: 0.30, high: 0.60 };

function shiftHourDecimal(shiftTime) {
  const s = String(shiftTime ?? '').padStart(4, '0');
  const h = parseInt(s.slice(0, 2), 10) + parseInt(s.slice(2, 4), 10) / 60;
  return Number.isNaN(h) ? null : h;
}

/** Resolve the traffic config for a city (profile override → city default). */
function resolveTrafficConfig(city, profile) {
  const key = getFastApiCityKey(city);
  return (profile && profile.trafficProfile && profile.trafficProfile[key]) ||
    CITY_TRAFFIC[key] || null;
}

/** Congestion level — 'low' | 'moderate' | 'high' — for a city at a shift time. */
function getTrafficLevel(city, shiftTime, profile) {
  const h = shiftHourDecimal(shiftTime);
  if (h == null) return 'moderate';
  const cfg = resolveTrafficConfig(city, profile);
  const inAny = ranges => Array.isArray(ranges) && ranges.some(([a, b]) => h >= a && h < b);
  if (cfg) {
    if (inAny(cfg.high)) return 'high';
    if (inAny(cfg.moderate)) return 'moderate';
    return 'low';
  }
  // Unknown city with no override — generic rush-hour fallback.
  if ((h >= 8.5 && h < 12.5) || (h >= 16.5 && h < 21)) return 'high';
  if (h >= 13 && h < 16) return 'moderate';
  return 'low';
}

/**
 * Traffic buffer — the fraction added to OSRM travel time for displayed route
 * durations and employee ETAs. City-aware: picks the city's congestion level at
 * shiftTime, then that city's buffer for that level. Does NOT touch the solver's
 * max_route_duration, which stays on raw OSRM time by design.
 */
function getTrafficBuffer(city, shiftTime, profile) {
  const cfg = resolveTrafficConfig(city, profile);
  const buffers = (cfg && cfg.buffers) || DEFAULT_TRAFFIC_BUFFERS;
  return buffers[getTrafficLevel(city, shiftTime, profile)] ?? 0.4;
}

function haversineKm([lat1, lon1], [lat2, lon2]) {
  const R = 6371;
  const toRad = d => d * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function formatTime(date) {
  if (!(date instanceof Date) || isNaN(date.getTime())) return '--:-- --';
  return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true });
}

/**
 * Bearing of an employee as seen from the facility, in degrees
 * (0 = North, clockwise, [0, 360)).
 */
function getBearingDeg(empLat, empLng, facLat, facLng) {
  const dLon = empLng - facLng;
  const dLat = empLat - facLat;
  return (Math.atan2(dLon, dLat) * 180 / Math.PI + 360) % 360;
}

/**
 * Returns which of `numSectors` angular sectors (0-based, clockwise from North)
 * the employee falls in when viewed from the facility.
 * Used to pre-cluster employees geographically before VRP, preventing
 * cross-city route groupings.
 */
function getBearingSector(empLat, empLng, facLat, facLng, numSectors = 8) {
  return Math.floor(getBearingDeg(empLat, empLng, facLat, facLng) / (360 / numSectors)) % numSectors;
}

// ─── OSRM: full N×N matrix ────────────────────────────────────────────────────

/**
 * Build full N×N distance + duration matrices via OSRM /table.
 * Automatically chunks into TABLE_CHUNK×TABLE_CHUNK sub-calls when N > OSRM_TABLE_LIMIT
 * to stay within the OSRM server's per-request coordinate cap.
 * nodes: [{lat, lng}, ...] — node[0] must be the depot (facility).
 * Returns { distances: number[][], durations: number[][] } in metres / seconds.
 */
async function buildMatrices(nodes, city) {
  const N = nodes.length;
  const fastApiCity = getFastApiCityKey(city);

  // ── Fast path: small enough for a single call ─────────────────────────────
  if (N <= OSRM_TABLE_LIMIT) {
    const coords = nodes.map(n => [n.lng, n.lat]);
    const res = await fetchApi(`${FASTAPI_GATEWAY_URL}/table`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ city: fastApiCity, coordinates: coords, annotations: 'duration,distance' }),
      timeout: 12000 + N * 300,
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      throw new Error(`OSRM /table HTTP ${res.status}: ${txt.slice(0, 200)}`);
    }
    const data = await res.json();
    if (data.code !== 'Ok' || !data.durations || !data.distances)
      throw new Error(`OSRM /table bad response code: ${data.code}`);
    return { distances: data.distances, durations: data.durations };
  }

  // ── Chunked path: split into TABLE_CHUNK×TABLE_CHUNK sub-matrices ─────────
  // Each call sends at most TABLE_CHUNK src + TABLE_CHUNK dst = 2×TABLE_CHUNK coords,
  // well within OSRM_TABLE_LIMIT.
  console.log(`[v3] Large matrix (${N} nodes) — chunked OSRM /table calls (chunk=${TABLE_CHUNK})`);

  const distances = Array.from({ length: N }, () => new Array(N).fill(0));
  const durations = Array.from({ length: N }, () => new Array(N).fill(0));

  // Build all (si, di) pairs upfront
  const pairs = [];
  for (let si = 0; si < N; si += TABLE_CHUNK)
    for (let di = 0; di < N; di += TABLE_CHUNK)
      pairs.push([si, di]);

  // Execute in parallel batches of 8 to avoid overwhelming the gateway
  const BATCH = 8;
  for (let b = 0; b < pairs.length; b += BATCH) {
    await Promise.all(pairs.slice(b, b + BATCH).map(async ([si, di]) => {
      const seEnd = Math.min(si + TABLE_CHUNK, N);
      const deEnd = Math.min(di + TABLE_CHUNK, N);
      const srcLen = seEnd - si;
      const dstLen = deEnd - di;

      let coords, srcIdx, dstIdx;
      if (si === di) {
        // Diagonal block: same set of nodes, no need to duplicate coords
        coords = nodes.slice(si, seEnd).map(n => [n.lng, n.lat]);
        srcIdx = Array.from({ length: srcLen }, (_, k) => k);
        dstIdx = Array.from({ length: srcLen }, (_, k) => k);
      } else {
        // Off-diagonal: concatenate src nodes + dst nodes
        coords = [
          ...nodes.slice(si, seEnd).map(n => [n.lng, n.lat]),
          ...nodes.slice(di, deEnd).map(n => [n.lng, n.lat]),
        ];
        srcIdx = Array.from({ length: srcLen }, (_, k) => k);
        dstIdx = Array.from({ length: dstLen }, (_, k) => srcLen + k);
      }

      const res = await fetchApi(`${FASTAPI_GATEWAY_URL}/table`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          city: fastApiCity,
          coordinates: coords,
          sources: srcIdx,
          destinations: dstIdx,
          annotations: 'duration,distance',
        }),
        timeout: 12000,
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`OSRM /table [${si}:${seEnd}]×[${di}:${deEnd}] HTTP ${res.status}: ${txt.slice(0, 150)}`);
      }
      const chunk = await res.json();
      if (chunk.code !== 'Ok' || !chunk.distances || !chunk.durations)
        throw new Error(`OSRM /table chunk bad response: ${chunk.code}`);

      // CRITICAL: the gateway IGNORES the `sources`/`destinations` params and
      // always returns the full coords×coords matrix. So for an off-diagonal
      // block (coords = [src nodes…, dst nodes…]) the destination columns live
      // at indices [srcLen … srcLen+dstLen-1], NOT [0 … dstLen-1]. Reading
      // chunk.distances[i][j] directly therefore pulled SOURCE-to-SOURCE
      // distances into the destination cells — corrupting every matrix with
      // >100 nodes (wrong facility-column depths → wrong clustering AND wrong
      // pickup sequencing → long backtracking routes). Index via srcIdx/dstIdx.
      // Stay robust if a future gateway honours the subset (srcLen×dstLen) form.
      const fullMatrix = chunk.distances.length === coords.length;
      for (let i = 0; i < srcLen; i++)
        for (let j = 0; j < dstLen; j++) {
          const ri = fullMatrix ? srcIdx[i] : i;
          const rj = fullMatrix ? dstIdx[j] : j;
          distances[si + i][di + j] = chunk.distances[ri]?.[rj] ?? 0;
          durations[si + i][di + j] = chunk.durations[ri]?.[rj] ?? 0;
        }
    }));
  }

  return { distances, durations };
}

// ─── OSRM: route geometry ─────────────────────────────────────────────────────

/**
 * OSRM /route for a finalised employee sequence.
 * coords: [[lat, lng], ...] in route order, including depot.
 * Returns { totalDistance (m), totalDuration (s, raw), encodedPolyline, legs[] }.
 */
async function getRouteGeometry(coords, city) {
  const fastApiCity = getFastApiCityKey(city);
  const osrmCoords = coords.map(([lat, lng]) => [lng, lat]);

  const res = await fetchApi(`${FASTAPI_GATEWAY_URL}/route`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      city: fastApiCity,
      coordinates: osrmCoords,
      overview: 'full',
      steps: true,
      geometries: 'polyline',
    }),
    timeout: 14000,
  });
  if (!res.ok) throw new Error(`OSRM /route HTTP ${res.status}`);
  const data = await res.json();
  if (data.code !== 'Ok' || !data.routes?.[0]) throw new Error('OSRM /route: no route returned');
  const r = data.routes[0];
  return {
    totalDistance: r.distance,
    totalDuration: r.duration,
    encodedPolyline: r.geometry || '',
    legs: r.legs || [],
  };
}

// ─── OR-Tools VRP subprocess ──────────────────────────────────────────────────

/**
 * Spawn or_tools_vrp_solver_v2.py via stdin/stdout JSON.
 * Returns { routes: [{vehicle_index, node_indices}], dropped_node_indices }.
 */
async function runVRPSolver(payload) {
  const pyExe = process.env.PYTHON_EXECUTABLE || 'python';
  const script = path.join(__dirname, 'or_tools_vrp_solver_v2.py');
  if (!fs.existsSync(script)) throw new Error(`VRP solver not found: ${script}`);

  // Hard ceiling on subprocess lifetime — OR-Tools is told to stop searching
  // after `solver_time_limit_seconds`, but a stuck/runaway process (bad input,
  // OR-Tools native hang, etc.) would otherwise block this group's batch
  // forever since `close` is the only resolution path. Give it the solver's
  // own budget plus generous startup/IPC slack, then kill it.
  const watchdogMs = ((payload.solver_time_limit_seconds || 30) + 45) * 1000;

  return new Promise((resolve, reject) => {
    const proc = spawn(pyExe, [script]);
    let stdout = '', stderr = '', settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      proc.kill('SIGKILL');
      reject(new Error(`VRP solver timed out after ${watchdogMs / 1000}s — killed subprocess`));
    }, watchdogMs);

    proc.stdin.write(JSON.stringify(payload));
    proc.stdin.end();
    proc.stdout.on('data', d => { stdout += d.toString(); });
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('close', code => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code !== 0 && !stdout.trim())
        return reject(new Error(`VRP exit ${code}: ${stderr.slice(0, 400)}`));
      try {
        const lines = stdout.trim().split('\n');
        let solution = null;
        for (let i = lines.length - 1; i >= 0; i--) {
          try { solution = JSON.parse(lines[i]); break; } catch {}
        }
        if (!solution) throw new Error('No JSON output from VRP solver');
        if (solution.error) throw new Error(`VRP solver: ${solution.error}`);
        resolve(solution);
      } catch (e) {
        reject(new Error(`VRP parse error: ${e.message} | stdout=${stdout.slice(0, 300)}`));
      }
    });
    proc.on('error', e => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`VRP spawn error: ${e.message}`));
    });
  });
}

// ─── vehicle selection ────────────────────────────────────────────────────────

/**
 * Smallest available vehicle whose capacity >= occupancy.
 * Falls back to the largest available vehicle when fleet is exhausted,
 * setting afterFleetExhaustion=true so the caller can flag the route.
 * Returns { type, capacity, count, afterFleetExhaustion }.
 */
function pickVehicle(occupancy, fleet, fleetUsed) {
  const byCapAsc  = [...fleet].sort((a, b) => a.capacity - b.capacity);
  const byCapDesc = [...fleet].sort((a, b) => b.capacity - a.capacity);

  for (const v of byCapAsc) {
    if (v.capacity >= occupancy && (fleetUsed[v.type] || 0) < v.count)
      return { ...v, afterFleetExhaustion: false };
  }
  // No fitting vehicle left — use the largest available to minimise capacity gap
  for (const v of byCapDesc) {
    if ((fleetUsed[v.type] || 0) < v.count)
      return { ...v, afterFleetExhaustion: true };
  }
  // Every type fully exhausted — fall back to type 'm' using ITS real capacity
  // from this facility's fleet (e.g. 5 for BDC/DDC, 6 in some profiles). Never
  // invent a capacity the input doesn't define. The overflow keeps all its
  // riders and is flagged afterFleetExhaustion.
  const mType = fleet.find(v => v.type === 'm');
  if (mType)
    return { type: 'm', capacity: mType.capacity, count: Infinity, afterFleetExhaustion: true };
  // 'm' not defined for this facility — use the largest available type instead.
  const largest = byCapDesc[0];
  if (largest)
    return { type: largest.type, capacity: largest.capacity, count: Infinity, afterFleetExhaustion: true };
  // Only reached if no fleet was supplied at all.
  return { type: 'm', capacity: 9, count: Infinity, afterFleetExhaustion: true };
}

// ─── guard swap ───────────────────────────────────────────────────────────────

/**
 * If the critical position (first for pickup / last for dropoff) is female,
 * try to swap with the nearest male employee within MAX_GUARD_SWAP_KM.
 * Returns { employees, swapped, guardNeeded }.
 */
async function tryGuardSwap(employees, isDropoff, facility, city, shiftTime) {
  const critIdx = isDropoff ? employees.length - 1 : 0;
  const crit = employees[critIdx];
  if (!crit || crit.gender !== 'F')
    return { employees, swapped: false, guardNeeded: false };

  // Pre-filter males within 2× road-distance threshold using haversine
  const males = employees
    .map((e, i) => ({ e, i }))
    .filter(({ e, i }) =>
      i !== critIdx &&
      e.gender === 'M' &&
      haversineKm([crit.location.lat, crit.location.lng], [e.location.lat, e.location.lng]) <= MAX_GUARD_SWAP_KM * 2
    );

  if (!males.length) return { employees, swapped: false, guardNeeded: true };

  try {
    const fastApiCity = getFastApiCityKey(city);
    const coords = [
      [crit.location.lng, crit.location.lat],
      ...males.map(({ e }) => [e.location.lng, e.location.lat]),
    ];
    const res = await fetchApi(`${FASTAPI_GATEWAY_URL}/table`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        city: fastApiCity, coordinates: coords,
        sources: [0],
        destinations: males.map((_, i) => i + 1),
        annotations: 'distance',
      }),
      timeout: 6000,
    });
    if (res.ok) {
      const data = await res.json();
      if (data.code === 'Ok' && data.distances?.[0]) {
        let bestIdx = -1, bestDist = Infinity;
        males.forEach(({ i }, ci) => {
          const km = (data.distances[0][ci + 1] ?? Infinity) / 1000;
          if (km <= MAX_GUARD_SWAP_KM && km < bestDist) { bestDist = km; bestIdx = i; }
        });
        if (bestIdx >= 0) {
          const emps = [...employees];
          [emps[critIdx], emps[bestIdx]] = [emps[bestIdx], emps[critIdx]];
          return {
            employees: emps,
            swapped: true,
            guardNeeded: false,
            swappedPairInfo: {
              movedToCritical: emps[critIdx].empCode,   // male now at first-pickup / last-drop
              movedFromCritical: emps[bestIdx].empCode, // female moved to the male's old slot
              roadDistanceKm: parseFloat(bestDist.toFixed(2)),
            },
          };
        }
      }
    }
  } catch {}

  return { employees, swapped: false, guardNeeded: true };
}

// ─── pickup / dropoff time calculation ───────────────────────────────────────

function applyPickupTimes(route, shiftTime, pickupTimePerEmp, reportingTime = 0, city) {
  if (!route.employees?.length || !shiftTime || !route.routeDetails?.legs) return;

  const buffer = Math.min(getTrafficBuffer(city, shiftTime), 0.8);
  const s = String(shiftTime).padStart(4, '0');
  const facilityDate = new Date();
  facilityDate.setHours(parseInt(s.slice(0, 2), 10), parseInt(s.slice(2, 4), 10), 0, 0);
  const facilityMs = facilityDate.getTime();

  const isDropoff = route.tripType?.toUpperCase() === 'DROPOFF';
  const legs = route.routeDetails.legs;

  if (!isDropoff) {
    // PICKUP: work backwards from facility arrival
    const arrivalMs = facilityMs - reportingTime * 1000;
    route.facilityArrivalTime = formatTime(new Date(arrivalMs));
    let ms = arrivalMs;
    for (let i = route.employees.length - 1; i >= 0; i--) {
      ms -= (legs[i]?.duration || 0) * (1 + buffer) * 1000;
      ms -= pickupTimePerEmp * 1000;
      route.employees[i].pickupTime = formatTime(new Date(ms));
    }
  } else {
    // DROPOFF: work forward from facility departure
    route.facilityDepartureTime = formatTime(facilityDate);
    let ms = facilityMs;
    for (let i = 0; i < route.employees.length; i++) {
      ms += (legs[i]?.duration || 0) * (1 + buffer) * 1000;
      ms += pickupTimePerEmp * 1000;
      const t = formatTime(new Date(ms));
      route.employees[i].dropoffTime = t;
      route.employees[i].pickupTime = t;
    }
  }
}

// ─── special employee routing (NMT / Medical / PWD) ──────────────────────────

/**
 * Routes NMT / Medical / PWD employees in small constrained groups,
 * separate from the main VRP run.
 */
async function routeSpecialEmployees({
  specials, facility, isDropoff, fleet, city,
  shiftTime, pickupTimePerEmp, reportingTime = 0, activateGuard, fleetUsed,
}) {
  if (!specials.length) return [];

  const facilityCoord = [facility.geoY, facility.geoX];
  const nmtCapacity = fleet.find(v => v.type === 's')?.capacity || 3;

  // Sort farthest-first for pickup, closest-first for dropoff
  const sorted = specials
    .map(emp => ({
      ...emp,
      _dist: haversineKm([emp.location.lat, emp.location.lng], facilityCoord),
    }))
    .sort((a, b) => isDropoff ? a._dist - b._dist : b._dist - a._dist);

  const routes = [];
  let i = 0;

  while (i < sorted.length) {
    const seed = sorted[i];
    const maxGroup = seed.isNMT ? nmtCapacity : 2;   // Medical/PWD capped at 2
    const group = [seed];
    i++;

    // Greedily add compatible close employees
    while (group.length < maxGroup && i < sorted.length) {
      const cand = sorted[i];
      const sameType =
        (seed.isNMT && cand.isNMT) ||
        ((seed.isMedical || seed.isPWD) && (cand.isMedical || cand.isPWD));
      if (!sameType) break;

      const gap = haversineKm(
        [group[group.length - 1].location.lat, group[group.length - 1].location.lng],
        [cand.location.lat, cand.location.lng]
      );
      if (gap > SPECIAL_GROUP_MAX_GAP_KM) break;

      group.push(cand);
      i++;
    }

    // Build ordered employee list
    let empList = group.map((e, idx) => ({ ...e, order: idx + 1 }));

    // Guard swap
    let guardNeeded = false, swapped = false, swappedPairInfo = null;
    if (activateGuard) {
      const g = await tryGuardSwap(empList, isDropoff, facility, city, shiftTime);
      empList = g.employees;
      guardNeeded = g.guardNeeded;
      swapped = g.swapped;
      swappedPairInfo = g.swappedPairInfo || null;
      // Re-number — swapped employees carry stale `order` values otherwise
      empList.forEach((e, i) => { e.order = i + 1; });
    }

    // Route geometry
    const empCoords = empList.map(e => [e.location.lat, e.location.lng]);
    const allCoords = isDropoff
      ? [facilityCoord, ...empCoords]
      : [...empCoords, facilityCoord];

    let routeDetails = { totalDistance: 0, totalDuration: 0, encodedPolyline: '', legs: [] };
    try {
      routeDetails = await getRouteGeometry(allCoords, city);
      routeDetails.totalDuration *= (1 + getTrafficBuffer(city, shiftTime));
    } catch (err) {
      console.warn(`[v3 special] Route geometry failed: ${err.message}`);
    }

    // Vehicle assignment
    const occupancy = empList.length + (guardNeeded ? 1 : 0);
    const { afterFleetExhaustion: afe, ...veh } = pickVehicle(occupancy, fleet, fleetUsed);
    fleetUsed[veh.type] = (fleetUsed[veh.type] || 0) + 1;

    const route = {
      routeNumber: 0,
      zone: seed.zone || 'SPECIAL',
      uniqueKey: `SPECIAL_${uuidv4()}`,
      tripType: isDropoff ? 'DROPOFF' : 'PICKUP',
      employees: empList,
      routeDetails,
      encodedPolyline: routeDetails.encodedPolyline,
      vehicleCapacity: veh.capacity,
      assignedVehicleType: veh.type,
      guardNeeded,
      swapped,
      swappedPairInfo: swappedPairInfo || null,
      isSpecialNeedsRoute: !seed.isNMT,
      isNMTRoute: seed.isNMT || false,
      afterFleetExhaustion: afe || false,
      error: false,
    };

    applyPickupTimes(route, shiftTime, pickupTimePerEmp, reportingTime, city);
    routes.push(route);
  }

  return routes;
}

// ─── CDC route deviation helpers ─────────────────────────────────────────────

/**
 * Find the CDC rule for an employee at `distKm` from the facility.
 * Returns the matching rule or the last rule if beyond all bands.
 */
function lookupCdcRule(cdcRules, distKm) {
  if (!cdcRules?.length) return null;
  for (const rule of cdcRules) {
    if (distKm >= rule.minDistKm && distKm <= rule.maxDistKm) return rule;
  }
  return cdcRules[cdcRules.length - 1];
}


// ─── 2-opt route improvement ─────────────────────────────────────────────────

/**
 * Improves the within-route ordering of a VRP solution using 2-opt.
 * Uses the actual OSRM road distance matrix, not haversine estimates.
 * Node 0 is always the depot (fixed at the end of a PICKUP route).
 *
 * nodeSeq : array of node indices (1-based) in current order.
 * distMatrix : full N×N distance matrix from buildMatrices (metres).
 * Returns an improved node sequence.
 */
function twoOptWithMatrix(nodeSeq, distMatrix, opts = {}) {
  const N = nodeSeq.length;
  if (N <= 3) return nodeSeq;

  // Direction penalty — MUST mirror the OR-Tools solver objective. Without it
  // 2-opt minimises pure road distance, which for a clustered cab can make an
  // "out-and-back" snake a few hundred metres cheaper than the monotonic order.
  // 2-opt then UNDOES the farthest-first depth ordering the caller just applied,
  // re-introducing the long backtracks. Re-applying the solver's backward-movement
  // penalty here keeps the post-pass consistent with the solver and preserves
  // linearity. Node 0 is the facility, so d(node, 0) = road depth to facility.
  const dirWeight = opts.directionPenaltyWeight ?? 0;
  const isDropoff = opts.isDropoff ?? false;

  function d(a, b) {
    return distMatrix[a]?.[b] ?? 999_999_999;
  }

  // OSRM road matrices are ASYMMETRIC (A→B ≠ B→A: one-ways, U-turns, ramps).
  // The classic 2-opt delta test d(a,c)+d(b,e) < d(a,b)+d(c,e) is only valid
  // for symmetric matrices — it ignores that reversing a segment also reverses
  // every arc inside it. On asymmetric data an "improving" swap can actually
  // worsen the route, so the loop can oscillate between two states forever
  // (this froze the whole Node event loop in production). Evaluating the FULL
  // route cost per candidate is correct for asymmetric matrices and guarantees
  // termination because accepted swaps strictly decrease a bounded cost.
  function pathCost(s) {
    let c = 0;
    for (let k = 0; k < s.length - 1; k++) {
      c += d(s[k], s[k + 1]);
      if (dirWeight > 0) {
        // depth = road distance from a stop to the facility (node 0)
        const depthFrom = d(s[k], 0);
        const depthTo   = d(s[k + 1], 0);
        // PICKUP: penalise moving AWAY from facility (depth increasing).
        // DROPOFF: penalise moving TOWARD facility (depth decreasing).
        const backward = isDropoff ? (depthFrom - depthTo) : (depthTo - depthFrom);
        if (backward > 0) c += dirWeight * backward;
      }
    }
    c += d(s[s.length - 1], 0);   // final leg to depot (node 0)
    return c;
  }

  let seq = [...nodeSeq];
  let bestCost = pathCost(seq);

  const MAX_PASSES = 25;          // hard cap — defense in depth
  let improved = true, passes = 0;
  while (improved && passes++ < MAX_PASSES) {
    improved = false;
    for (let i = 0; i < N - 1; i++) {
      for (let j = i + 2; j < N; j++) {
        // Candidate: reverse segment [i+1..j]
        const cand = seq.slice();
        let l = i + 1, r = j;
        while (l < r) { [cand[l], cand[r]] = [cand[r], cand[l]]; l++; r--; }

        const c = pathCost(cand);
        if (c < bestCost - 1) {
          seq = cand;
          bestCost = c;
          improved = true;
        }
      }
    }
  }
  return seq;
}

// ─── per-sector VRP runner ────────────────────────────────────────────────────

/**
 * Runs the full pipeline (OSRM matrix → VRP → guard swap → geometry) for one
 * geographic sector group. Returns partial route objects ready for vehicle
 * assignment, plus any employees that the solver dropped.
 */
async function runVRPForGroup({
  employees, facility, facilityCoord, isDropoff,
  maxDuration, dirPenaltyWeight, lateralPenaltyWeight, fleet, city,
  shiftTime, pickupTimePerEmployee, activateGuard, cdcRules,
}) {
  if (!employees.length) return { partials: [], dropped: [] };

  const tGroup = Date.now();
  const matrixNodes = [
    { lat: facility.geoY, lng: facility.geoX },
    ...employees.map(e => ({ lat: e.location.lat, lng: e.location.lng })),
  ];

  let distances, durations;
  try {
    const m = await buildMatrices(matrixNodes, city);
    distances = m.distances;
    durations = m.durations;
  } catch (err) {
    console.error(`[v3] Matrix failed for group(${employees.length}): ${err.message}`);
    return { partials: [], dropped: employees };
  }

  const sortedFleetDesc = [...fleet].sort((a, b) => b.capacity - a.capacity);
  const maxCapacity = sortedFleetDesc[0].capacity;

  // Cap vehicles at 2× the theoretical minimum + a small buffer.
  // Passing the full fleet (e.g. 152) gives OR-Tools so many vehicles that it
  // trivially assigns 1 employee per vehicle and achieves a low-cost solution
  // with terrible occupancy. Capping forces denser packing.
  const minRoutes = Math.ceil(employees.length / maxCapacity);
  const targetVehicles = Math.min(minRoutes * 2 + 4, employees.length);
  const vehicleCapacities = [];
  for (const v of sortedFleetDesc) {
    for (let k = 0; k < v.count && vehicleCapacities.length < targetVehicles; k++)
      vehicleCapacities.push(v.capacity);
    if (vehicleCapacities.length >= targetVehicles) break;
  }
  while (vehicleCapacities.length < targetVehicles)
    vehicleCapacities.push(sortedFleetDesc[sortedFleetDesc.length - 1].capacity);

  // OR-Tools guided local search ALWAYS runs for the full time limit — it never
  // returns early. So this budget must be proportional to problem size: giving a
  // 5-employee group 20s means 20s of dead waiting. Small groups converge to
  // optimal in 1-2s; only large groups benefit from a bigger budget.
  const timeLimitSeconds =
    employees.length <= 8   ? 3  :
    employees.length <= 15  ? 6  :
    employees.length <= 30  ? 10 :
    employees.length <= 60  ? 20 :
    employees.length <= 100 ? 30 : 45;

  // Use maxDuration directly. The traffic buffer is applied post-hoc to displayed
  // durations only — it should not shrink the solver's hard constraint, which
  // would cause mass employee drops when pickupTimePerEmployee is high.
  const solverMaxDuration = maxDuration;

  // Polar coordinates of every node relative to the facility, for the solver's
  // cross-track (lateral) penalty: bearing in degrees + straight-line depth in
  // metres. Depot is node 0 (bearing/depth 0 — never penalised).
  const nodeBearingsDeg = [0, ...employees.map(e =>
    getBearingDeg(e.location.lat, e.location.lng, facility.geoY, facility.geoX)
  )];
  const nodeDepthsM = [0, ...employees.map(e =>
    haversineKm([e.location.lat, e.location.lng], facilityCoord) * 1000
  )];

  let vrpResult;
  try {
    vrpResult = await runVRPSolver({
      distance_matrix:           distances,
      duration_matrix:           durations,
      num_vehicles:              vehicleCapacities.length,
      vehicle_capacities:        vehicleCapacities,
      demands:                   [0, ...employees.map(() => 1)],
      service_times:             [0, ...employees.map(() => pickupTimePerEmployee)],
      depot_index:               0,
      max_route_duration:        solverMaxDuration,
      trip_type:                 isDropoff ? 'DROPOFF' : 'PICKUP',
      direction_penalty_weight:  dirPenaltyWeight,
      lateral_penalty_weight:    lateralPenaltyWeight,
      node_bearings_deg:         nodeBearingsDeg,
      node_depths_m:             nodeDepthsM,
      allow_dropping_visits:     true,
      drop_visit_penalty:        100_000_000,
      solver_time_limit_seconds: timeLimitSeconds,
    });
  } catch (err) {
    console.error(`[v3] VRP failed for group(${employees.length}): ${err.message}`);
    return { partials: [], dropped: employees };
  }

  console.log(`[v3] Group(${employees.length}) solved: ${(vrpResult.routes || []).length} routes in ${((Date.now() - tGroup) / 1000).toFixed(1)}s`);

  const droppedSet = new Set(vrpResult.dropped_node_indices || []);
  const dropped = employees.filter((_, i) => droppedSet.has(i + 1));
  if (dropped.length)
    console.warn(`[v3] Dropped ${dropped.length}: ${dropped.map(e => e.empCode).join(', ')}`);

  // Per-route: 2-opt → CDC trim → guard swap → geometry
  const partialPromises = (vrpResult.routes || []).map(async (vrpRoute) => {
    const nodeIndices = vrpRoute.node_indices || [];
    if (!nodeIndices.length) return null;

    const validIndices = nodeIndices.filter(idx => idx >= 1 && idx <= employees.length);
    if (!validIndices.length) return null;

    // Step 1 — Pre-sort by road distance to facility (farthest first for PICKUP,
    // closest first for DROPOFF). OR-Tools minimises total distance without
    // enforcing linearity; starting from a monotonic depth ordering means
    // 2-opt only makes small local swaps, never large cross-city reversals.
    const depthSorted = validIndices.slice().sort((a, b) => {
      const da = distances[a]?.[0] ?? 0;   // road dist: employee a → facility
      const db = distances[b]?.[0] ?? 0;
      return isDropoff ? da - db : db - da; // pickup: farthest first; dropoff: nearest first
    });

    // Step 2 — 2-opt: fine-tune using actual OSRM road distances. Starting from
    // the monotonic depth-sorted order keeps 2-opt to small local swaps. The
    // backward-movement penalty (mirroring the solver) keeps it from trading the
    // monotonic order for a tiny raw-distance saving.
    let seq = twoOptWithMatrix(depthSorted, distances, {
      directionPenaltyWeight: dirPenaltyWeight,
      isDropoff,
    });

    // Step 3 — Build employee list
    const emps = seq.map(idx => ({ ...employees[idx - 1] }));
    emps.forEach((e, i) => { e.order = i + 1; });

    // Step 4 — Guard swap
    let finalEmps = emps;
    let guardNeeded = false, swapped = false, swappedPairInfo = null;
    if (activateGuard) {
      const g = await tryGuardSwap(finalEmps, isDropoff, facility, city, shiftTime);
      finalEmps = g.employees;
      guardNeeded = g.guardNeeded;
      swapped = g.swapped;
      swappedPairInfo = g.swappedPairInfo || null;
      // Re-number after the swap — each employee object carries its `order`
      // field with it, so without this the swapped pair keeps stale positions
      // (female still shown as stop 1 in the UI while the cab actually picks
      // up the male first).
      finalEmps.forEach((e, i) => { e.order = i + 1; });
    }

    // Step 5 — OSRM route geometry
    const empCoords = finalEmps.map(e => [e.location.lat, e.location.lng]);
    const allCoords = isDropoff
      ? [facilityCoord, ...empCoords]
      : [...empCoords, facilityCoord];

    let routeDetails = { totalDistance: 0, totalDuration: 0, encodedPolyline: '', legs: [] };
    try {
      routeDetails = await getRouteGeometry(allCoords, city);
      routeDetails.totalDuration *= (1 + getTrafficBuffer(city, shiftTime));
    } catch (err) {
      console.warn(`[v3] Geometry failed: ${err.message}`);
    }

    const zoneCounts = {};
    finalEmps.forEach(e => { zoneCounts[e.zone] = (zoneCounts[e.zone] || 0) + 1; });
    const zone = Object.entries(zoneCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || 'DEFAULT_ZONE';

    // Linearity metric — detour ratio: actual route road distance vs the direct
    // road distance of the farthest stop. ~1.1-1.3 = near-perfectly linear;
    // 1.8+ = snaking. Lets route quality be compared objectively across
    // penalty-weight configurations instead of eyeballing maps.
    const farthestRoadM = Math.max(...seq.map(idx =>
      isDropoff ? (distances[0]?.[idx] ?? 0) : (distances[idx]?.[0] ?? 0)
    ));
    const detourRatio = farthestRoadM > 0 && routeDetails.totalDistance > 0
      ? parseFloat((routeDetails.totalDistance / farthestRoadM).toFixed(2))
      : null;

    const durationExceeded = routeDetails.totalDuration > maxDuration;
    if (durationExceeded)
      console.warn(`[v3] Route duration ${Math.round(routeDetails.totalDuration)}s exceeds max ${maxDuration}s`);

    // Step 6 — CDC deviation flag (informational only — all employees stay routed)
    // Business rule: every rostered employee must be routed regardless of deviation.
    // deviationExceeded=true lets ops teams know the route breached the facility's
    // distance-band limit; it does NOT cause any employee to be dropped.
    let deviationExceeded = false;
    if (cdcRules?.length) {
      const routeDistKm = (routeDetails.totalDistance || 0) / 1000;
      let farthestDistKm = 0;
      for (const emp of finalEmps) {
        const d = haversineKm([emp.location.lat, emp.location.lng], facilityCoord);
        if (d > farthestDistKm) farthestDistKm = d;
      }
      const rule = lookupCdcRule(cdcRules, farthestDistKm);
      if (rule && routeDistKm > rule.maxTotalOneWayKm) {
        deviationExceeded = true;
        console.warn(
          `[v3 CDC] Route deviation exceeded: ` +
          `routeDist=${routeDistKm.toFixed(1)}km > max=${rule.maxTotalOneWayKm}km ` +
          `(farthest emp=${farthestDistKm.toFixed(1)}km from facility)`
        );
      }
    }

    return { finalEmps, guardNeeded, swapped, swappedPairInfo, routeDetails, zone, durationExceeded, deviationExceeded, detourRatio };
  });

  const partials = (await Promise.all(partialPromises)).filter(p => p?.finalEmps?.length);
  return { partials, dropped };
}

// ─── unrouted recovery ────────────────────────────────────────────────────────
//
// Guarantees no valid-coordinate employee is left unrouted. Runs ONLY when the
// main pipeline dropped someone (capacity/duration infeasibility, or a whole-
// group OSRM/VRP error). Two phases, cheapest-first:
//   1. Insert each dropped employee into an EXISTING nearby route that has a
//      spare seat, IF the result still respects maxDuration (the solver's hard
//      constraint) and the route-deviation band. Adds no vehicles.
//   2. Whatever can't be cleanly inserted → new proximity-grouped / singleton
//      routes. These always route the employee (the "no one unrouted" rule wins
//      over the soft deviation limit), flagged so ops can see the overflow.
// Only employees with missing/invalid coordinates remain unrouted.

/** Straight-line length (km) of an ordered employee route including the facility leg. */
function _recPathLenKm(orderedEmps, facilityCoord, isDropoff) {
  const pts = orderedEmps.map(e => [e.location.lat, e.location.lng]);
  const seq = isDropoff ? [facilityCoord, ...pts] : [...pts, facilityCoord];
  let L = 0;
  for (let i = 0; i < seq.length - 1; i++) L += haversineKm(seq[i], seq[i + 1]);
  return L;
}

/** True if the route's road distance breaches its CDC deviation band. */
function _recDeviationExceeded(cdcRules, totalDistanceM, emps, facilityCoord) {
  if (!cdcRules?.length) return false;
  const routeDistKm = (totalDistanceM || 0) / 1000;
  let farthest = 0;
  for (const e of emps) {
    const d = haversineKm([e.location.lat, e.location.lng], facilityCoord);
    if (d > farthest) farthest = d;
  }
  const rule = lookupCdcRule(cdcRules, farthest);
  return !!(rule && routeDistKm > rule.maxTotalOneWayKm);
}

/** Detour ratio using straight-line farthest depth (no road matrix available here). */
function _recDetour(totalDistanceM, emps, facilityCoord) {
  let farM = 0;
  for (const e of emps) {
    const d = haversineKm([e.location.lat, e.location.lng], facilityCoord) * 1000;
    if (d > farM) farM = d;
  }
  return farM > 0 && totalDistanceM > 0
    ? parseFloat((totalDistanceM / farM).toFixed(2))
    : null;
}

/**
 * Apply guard swap (if active) to an ordered list, then fetch OSRM geometry.
 * Returns { emps, raw, guardNeeded, swapped, swappedPairInfo }. `raw` carries
 * un-buffered OSRM durations (legs stay raw for applyPickupTimes).
 */
async function _recBuildOrdered(orderedEmps, ctx) {
  let emps = orderedEmps;
  let guardNeeded = false, swapped = false, swappedPairInfo = null;
  if (ctx.activateGuard) {
    const g = await tryGuardSwap(emps, ctx.isDropoff, ctx.facility, ctx.city, ctx.shiftTime);
    emps = g.employees;
    guardNeeded = g.guardNeeded;
    swapped = g.swapped;
    swappedPairInfo = g.swappedPairInfo || null;
  }
  emps = emps.map((e, i) => ({ ...e, order: i + 1 }));

  const empCoords = emps.map(e => [e.location.lat, e.location.lng]);
  const allCoords = ctx.isDropoff ? [ctx.facilityCoord, ...empCoords] : [...empCoords, ctx.facilityCoord];
  let raw = { totalDistance: 0, totalDuration: 0, encodedPolyline: '', legs: [] };
  try {
    raw = await getRouteGeometry(allCoords, ctx.city);
  } catch (err) {
    console.warn(`[v3 recovery] geometry failed: ${err.message}`);
  }
  return { emps, raw, guardNeeded, swapped, swappedPairInfo };
}

/** Write computed geometry/flags/times onto a route object (buffered duration for display). */
function _recWriteRouteDetails(route, built, ctx) {
  const bufferedDuration = (built.raw.totalDuration || 0) * (1 + getTrafficBuffer(ctx.city, ctx.shiftTime));
  route.employees      = built.emps;
  route.guardNeeded    = built.guardNeeded;
  route.swapped        = built.swapped || route.swapped || false;
  route.swappedPairInfo = built.swappedPairInfo || route.swappedPairInfo || null;
  route.routeDetails   = { ...built.raw, totalDuration: bufferedDuration };  // legs stay raw
  route.encodedPolyline = built.raw.encodedPolyline || '';
  route.durationExceeded  = bufferedDuration > ctx.maxDuration;
  route.deviationExceeded = _recDeviationExceeded(ctx.cdcRules, built.raw.totalDistance, built.emps, ctx.facilityCoord);
  route.detourRatio    = _recDetour(built.raw.totalDistance, built.emps, ctx.facilityCoord);
  route.recovered      = true;
  applyPickupTimes(route, ctx.shiftTime, ctx.pickupTimePerEmployee, ctx.reportingTime, ctx.city);
}

/**
 * Phase 1 — try to insert `emp` into the cheapest feasible existing route.
 * Mutates the chosen route in place and returns true on success.
 */
async function _recTryInsert(emp, routes, ctx) {
  const empLL = [emp.location.lat, emp.location.lng];

  // Candidate routes: regular (non-special), have a spare seat, and pass near emp.
  const cands = [];
  for (const r of routes) {
    if (r.isSpecialNeedsRoute || r.isNMTRoute) continue;
    if (r.employees.length >= r.vehicleCapacity) continue;          // no spare seat
    let minGap = Infinity;
    for (const e of r.employees) {
      const d = haversineKm(empLL, [e.location.lat, e.location.lng]);
      if (d < minGap) minGap = d;
    }
    if (minGap <= RECOVERY_MAX_INSERT_GAP_KM) cands.push({ r, minGap });
  }
  cands.sort((a, b) => a.minGap - b.minGap);   // nearest cab first

  let tries = 0;
  for (const { r } of cands) {
    if (tries >= RECOVERY_MAX_OSRM_TRIES) break;

    // Cheapest insertion index by straight-line path length (cheap, no OSRM).
    const base = r.employees;
    let bestIdx = 0, bestLen = Infinity;
    for (let p = 0; p <= base.length; p++) {
      const order = [...base.slice(0, p), emp, ...base.slice(p)];
      const len = _recPathLenKm(order, ctx.facilityCoord, ctx.isDropoff);
      if (len < bestLen) { bestLen = len; bestIdx = p; }
    }
    const order = [...base.slice(0, bestIdx), emp, ...base.slice(bestIdx)];

    tries++;
    const built = await _recBuildOrdered(order, ctx);

    // Capacity must still hold once a guard seat (if newly needed) is counted.
    if (built.emps.length + (built.guardNeeded ? 1 : 0) > r.vehicleCapacity) continue;

    // maxDuration is the solver's HARD constraint — enforce on raw duration +
    // per-stop service time, exactly as the solver does. Deviation is soft: we
    // skip insertions that breach the band (a fresh route usually won't).
    const rawDuration = (built.raw.totalDuration || 0) + ctx.pickupTimePerEmployee * built.emps.length;
    if (rawDuration > ctx.maxDuration) continue;
    if (_recDeviationExceeded(ctx.cdcRules, built.raw.totalDistance, built.emps, ctx.facilityCoord)) continue;

    _recWriteRouteDetails(r, built, ctx);   // commit
    return true;
  }
  return false;
}

/**
 * Phase 2 — route the leftovers into NEW proximity-grouped / singleton routes.
 * These are the "guarantee" routes; they always route the employee even if a
 * limit is exceeded (flagged), since leaving anyone unrouted is the harder rule.
 */
async function _recCreateNewRoutes(emps, ctx, fleet, fleetUsed, tripType) {
  const created = [];
  const maxCap = Math.max(...fleet.map(v => v.capacity));

  // Depth order (farthest-first for pickup) so chaining sweeps inward.
  const sorted = [...emps].sort((a, b) => {
    const da = haversineKm([a.location.lat, a.location.lng], ctx.facilityCoord);
    const db = haversineKm([b.location.lat, b.location.lng], ctx.facilityCoord);
    return ctx.isDropoff ? da - db : db - da;
  });

  // Greedy nearest-neighbour grouping, capped at the largest vehicle, gap-limited.
  const used = new Array(sorted.length).fill(false);
  const groups = [];
  for (let i = 0; i < sorted.length; i++) {
    if (used[i]) continue;
    used[i] = true;
    const group = [sorted[i]];
    let lastLL = [sorted[i].location.lat, sorted[i].location.lng];
    while (group.length < maxCap) {
      let bi = -1, bd = Infinity;
      for (let j = 0; j < sorted.length; j++) {
        if (used[j]) continue;
        const d = haversineKm(lastLL, [sorted[j].location.lat, sorted[j].location.lng]);
        if (d < bd && d <= RECOVERY_NEW_GROUP_MAX_GAP_KM) { bd = d; bi = j; }
      }
      if (bi < 0) break;
      used[bi] = true;
      group.push(sorted[bi]);
      lastLL = [sorted[bi].location.lat, sorted[bi].location.lng];
    }
    groups.push(group);
  }

  for (const group of groups) {
    // Within-group order: depth sort (monotonic toward facility).
    const order = [...group].sort((a, b) => {
      const da = haversineKm([a.location.lat, a.location.lng], ctx.facilityCoord);
      const db = haversineKm([b.location.lat, b.location.lng], ctx.facilityCoord);
      return ctx.isDropoff ? da - db : db - da;
    });
    const built = await _recBuildOrdered(order, ctx);
    const occupancy = built.emps.length + (built.guardNeeded ? 1 : 0);
    const { afterFleetExhaustion: afe, ...veh } = pickVehicle(occupancy, fleet, fleetUsed);
    fleetUsed[veh.type] = (fleetUsed[veh.type] || 0) + 1;

    const route = {
      routeNumber: 0,
      zone: built.emps[0].zone || 'RECOVERY',
      uniqueKey: `v3_recovered_${uuidv4()}`,
      tripType,
      vehicleCapacity: veh.capacity,
      assignedVehicleType: veh.type,
      isSpecialNeedsRoute: false,
      isNMTRoute: false,
      afterFleetExhaustion: afe || false,
      error: false,
    };
    _recWriteRouteDetails(route, built, ctx);
    created.push(route);
  }
  return created;
}

/**
 * Entry point. Inserts/creates routes for dropped employees (mutating allRoutes
 * in place and appending new routes), and returns the list that genuinely cannot
 * be routed (missing/invalid coordinates only).
 */
async function recoverUnroutedEmployees({
  unrouted, allRoutes, facility, facilityCoord, isDropoff, city, shiftTime,
  pickupTimePerEmployee, reportingTime, maxDuration, cdcRules, activateGuard,
  tripType, fleet, fleetUsed,
}) {
  const recoverable = unrouted.filter(e => e.location && e.location.lat && e.location.lng);
  const invalid     = unrouted.filter(e => !(e.location && e.location.lat && e.location.lng));
  if (!recoverable.length) return invalid;

  const tRec = Date.now();
  const ctx = {
    facility, facilityCoord, isDropoff, city, shiftTime,
    pickupTimePerEmployee, reportingTime, maxDuration, cdcRules, activateGuard,
  };

  // Hardest (farthest) first — the most constrained employees get first pick of
  // existing spare seats before we resort to new vehicles.
  recoverable.sort((a, b) =>
    haversineKm([b.location.lat, b.location.lng], facilityCoord) -
    haversineKm([a.location.lat, a.location.lng], facilityCoord)
  );

  let inserted = 0;
  const leftover = [];
  for (const emp of recoverable) {
    const ok = await _recTryInsert(emp, allRoutes, ctx);
    if (ok) inserted++; else leftover.push(emp);
  }

  let newRoutes = [];
  if (leftover.length) {
    newRoutes = await _recCreateNewRoutes(leftover, ctx, fleet, fleetUsed, tripType);
    allRoutes.push(...newRoutes);
  }

  console.log(
    `[v3 recovery] ${recoverable.length} dropped → inserted ${inserted}, ` +
    `${newRoutes.length} new route(s); ${invalid.length} unrouted (invalid coords) ` +
    `in ${((Date.now() - tRec) / 1000).toFixed(1)}s`
  );
  return invalid;
}

// ─── main entry point ─────────────────────────────────────────────────────────

async function generateRoutes(data) {
  const {
    employees: rawEmps = [],
    facility,
    shiftTime,
    date,
    profile = {},
    tripType: rawTripType = 'PICKUP',
    pickupTimePerEmployee = 180,
    reportingTime = 0,
    guard: activateGuard = false,
  } = data;

  if (!rawEmps.length) throw new Error('No employees provided');
  if (!facility?.geoX || !facility?.geoY) throw new Error('Invalid facility coordinates');
  if (!shiftTime || !date) throw new Error('shiftTime and date are required');

  // Normalise trip type
  const t = String(rawTripType).toUpperCase();
  const tripType = t === 'P' ? 'PICKUP' : t === 'D' ? 'DROPOFF' : (t === 'DROPOFF' ? 'DROPOFF' : 'PICKUP');
  const isDropoff = tripType === 'DROPOFF';
  const facilityCoord = [facility.geoY, facility.geoX];

  const city = profile.name || 'ncr';
  const maxDuration = profile.maxDuration || 7200;
  const dirPenaltyWeight = profile.directionPenaltyWeight || 15.0;
  // Cross-track penalty: punishes SIDEWAYS movement (perpendicular to the
  // facility direction), shaping routes into narrow linear "petals".
  // The direction penalty alone only punishes backward movement — the solver
  // could still snake laterally across a 45° sector at no extra cost.
  // Higher = straighter routes but lower occupancy (more vehicles). Tunable
  // per client profile. Measured on an 87-emp Bengaluru shift:
  //   3.0 → 16 routes, occ 5.44, avg detour 1.34  (max packing)
  //   4.0 → 18 routes, occ 4.83, avg detour 1.30  (matches legacy engine's shape
  //         at ~7 km less total distance — chosen default)
  //   6.0 → 23 routes, occ 3.78, avg detour 1.18  (very linear, +14% distance)
  const lateralPenaltyWeight = profile.lateralPenaltyWeight ?? 4.0;
  const fleet = (profile.fleet?.length ? profile.fleet : [{ type: 'm', capacity: 9, count: 9999 }]);
  // Take the first (and typically only) ruleset from routeDeviationRules regardless
  // of the key name — it could be "CDC", "DC", "BDC", etc. depending on the facility.
  const cdcRules = profile.routeDeviationRules
    ? Object.values(profile.routeDeviationRules)[0] || null
    : null;

  console.log(`[v3] ─── Start: ${rawEmps.length} emps | shift=${shiftTime} | ${tripType} | city=${city} ───`);

  // ── Enrich + validate employees ──
  const allEmps = rawEmps.map(emp => ({
    ...emp,
    isMedical: emp.isMedical || false,
    isPWD:     emp.isPWD     || false,
    isNMT:     emp.isNMT     || false,
    isOOB:     emp.isOOB     || false,
    zone:      emp.zone      || 'DEFAULT_ZONE',
    location:  { lat: parseFloat(emp.geoY), lng: parseFloat(emp.geoX) },
  }));

  const validEmps   = allEmps.filter(e => e.location.lat && e.location.lng);
  const invalidEmps = allEmps.filter(e => !e.location.lat || !e.location.lng);
  if (invalidEmps.length)
    console.warn(`[v3] ${invalidEmps.length} employees have missing coordinates and will be unrouted`);

  // ── Separate special employees ──
  const specials = validEmps.filter(e => e.isNMT || e.isMedical || e.isPWD);
  const regulars = validEmps.filter(e => !e.isNMT && !e.isMedical && !e.isPWD);
  console.log(`[v3] Regular: ${regulars.length} | Special: ${specials.length}`);

  const fleetUsed = {};   // { vehicleType → count used }
  const unrouted  = [...invalidEmps];

  // ── Route special employees ──
  const specialRoutes = await routeSpecialEmployees({
    specials, facility, isDropoff, fleet, city,
    shiftTime, pickupTimePerEmp: pickupTimePerEmployee,
    reportingTime, activateGuard, fleetUsed,
  });
  console.log(`[v3] Special routes created: ${specialRoutes.length}`);

  // ── Route regular employees via OR-Tools (sector-per-sector) ──
  const regularRoutes = [];

  if (regulars.length > 0) {

    // Assign each employee to one of 8 angular bearing sectors from the facility.
    // Employees in different sectors (different directions from facility) are
    // never grouped together, eliminating cross-city zigzag routes structurally.
    regulars.forEach(emp => {
      emp._sector = getBearingSector(
        emp.location.lat, emp.location.lng,
        facilityCoord[0], facilityCoord[1]
      );
    });

    const rawSectors = {};
    for (let s = 0; s < 8; s++) rawSectors[s] = [];
    regulars.forEach(emp => rawSectors[emp._sector].push(emp));

    // Sub-divide each 45° bearing sector into 8 km distance bands.
    // Without this, a single "north" sector spans employees at 2 km and 45 km
    // from the facility, and OR-Tools freely mixes them — producing routes that
    // travel north 40 km, back south 30 km, north again (zigzag).
    // With distance bands every VRP group is both directionally coherent (same
    // bearing) AND depth-coherent (same road depth from depot), so OR-Tools can
    // only produce linear sweep routes within each group.
    const DIST_BAND_KM = 8;
    const minGroupSize = Math.min(...fleet.map(v => v.capacity));
    // Only sub-divide a sector when it actually spans a wide depth range —
    // that's the scenario that causes zigzag (e.g. one sector holding employees
    // from 2 km to 45 km away). Narrow sectors (typical for smaller employee
    // counts / compact cities) gain nothing from splitting — it would only
    // fragment them into tiny groups and multiply OSRM + VRP overhead.
    const SPLIT_DEPTH_THRESHOLD_KM = DIST_BAND_KM * 2;

    const sectorGroups = [];
    for (let s = 0; s < 8; s++) {
      const sector = rawSectors[s];
      if (!sector.length) continue;

      const sectorDists = sector.map(emp =>
        haversineKm([emp.location.lat, emp.location.lng], facilityCoord)
      );
      const depthRange = Math.max(...sectorDists) - Math.min(...sectorDists);

      // Skip splitting: sector is geographically narrow, or too small to form
      // more than one full group anyway — keep it intact as a single VRP group.
      if (depthRange <= SPLIT_DEPTH_THRESHOLD_KM || sector.length < minGroupSize * 2) {
        sectorGroups.push(sector);
        continue;
      }

      // Bucket employees by 8 km haversine depth from facility
      const bands = {};
      for (const emp of sector) {
        const band = Math.floor(
          haversineKm([emp.location.lat, emp.location.lng], facilityCoord) / DIST_BAND_KM
        );
        if (!bands[band]) bands[band] = [];
        bands[band].push(emp);
      }

      // Merge bands that are too small into the next deeper band so we never
      // pass a tiny group to OR-Tools and waste a vehicle on 1-2 employees.
      const sortedKeys = Object.keys(bands).map(Number).sort((a, b) => a - b);
      const merged = [];
      let pending = [];
      for (const k of sortedKeys) {
        pending.push(...bands[k]);
        if (pending.length >= minGroupSize) {
          merged.push([...pending]);
          pending = [];
        }
      }
      // Any leftover (last band too small): absorb into the previous group
      if (pending.length) {
        if (merged.length) merged[merged.length - 1].push(...pending);
        else merged.push(pending);
      }

      sectorGroups.push(...merged);
    }

    console.log(`[v3] Groups: ${sectorGroups.length} sector×band groups, sizes: [${sectorGroups.map(g => g.length).join(', ')}]`);

    // Pre-allocate fleet proportionally across sectors.
    // Without this, each sector independently sees the full fleet in the VRP
    // (e.g. l.count=5 per sector × 7 sectors = 35 large vehicles planned),
    // causing more large-vehicle routes than actually exist. Proportional
    // allocation ensures total planned ≤ total available across all sectors.
    const totalReg = regulars.length;
    const sectorFleets = sectorGroups.map(g => {
      const frac = g.length / totalReg;
      // floor() intentionally — under-allocate rather than over-allocate.
      // Sectors that round to 0 get no vehicles of that type; they use the
      // next smaller type instead. The smallest vehicle type always gets at
      // least 1 so every sector can route its employees.
      // Find smallest-capacity type by value — the request's fleet array order
      // is not guaranteed (some clients send ascending, some descending).
      const smallestIdx = fleet.reduce(
        (mi, v, i) => (v.capacity < fleet[mi].capacity ? i : mi), 0
      );
      return fleet.map((v, vi) => ({
        ...v,
        count: vi === smallestIdx                 // smallest type always ≥ 1
          ? Math.max(1, Math.floor(v.count * frac))
          : Math.floor(v.count * frac),
      }));
    });
    // Distribute any remaining fleet units (from flooring) to the largest sectors
    fleet.forEach(v => {
      let remaining = v.count - sectorFleets.reduce((s, sf) => s + sf.find(x => x.type === v.type).count, 0);
      [...sectorGroups.keys()]
        .sort((a, b) => sectorGroups[b].length - sectorGroups[a].length)  // largest first
        .forEach(idx => {
          if (remaining <= 0) return;
          sectorFleets[idx].find(x => x.type === v.type).count++;
          remaining--;
        });
    });

    // Run VRP independently per sector, in parallel batches of 4
    const allPartials = [];
    const SECTOR_BATCH = 4;
    for (let b = 0; b < sectorGroups.length; b += SECTOR_BATCH) {
      console.log(`[v3] Solving groups ${b + 1}-${Math.min(b + SECTOR_BATCH, sectorGroups.length)} of ${sectorGroups.length}...`);
      const batchResults = await Promise.all(
        sectorGroups.slice(b, b + SECTOR_BATCH).map((group, i) =>
          runVRPForGroup({
            employees: group, facility, facilityCoord, isDropoff,
            maxDuration, dirPenaltyWeight, lateralPenaltyWeight,
            fleet: sectorFleets[b + i], city,
            shiftTime, pickupTimePerEmployee, activateGuard, cdcRules,
          })
        )
      );
      batchResults.forEach(({ partials: ps, dropped: dp }) => {
        allPartials.push(...ps);
        unrouted.push(...dp);
      });
    }

    // Vehicle assignment SEQUENTIALLY (fleet counter must be consistent)
    for (const partial of allPartials) {
      const { finalEmps, guardNeeded, swapped, swappedPairInfo, routeDetails, zone, durationExceeded, deviationExceeded, detourRatio } = partial;

      const occupancy = finalEmps.length + (guardNeeded ? 1 : 0);
      const { afterFleetExhaustion: afe, ...veh } = pickVehicle(occupancy, fleet, fleetUsed);
      fleetUsed[veh.type] = (fleetUsed[veh.type] || 0) + 1;

      const route = {
        routeNumber: 0,
        zone,
        uniqueKey: `v3_${uuidv4()}`,
        tripType,
        employees: finalEmps,
        routeDetails,
        encodedPolyline: routeDetails.encodedPolyline,
        vehicleCapacity: veh.capacity,
        assignedVehicleType: veh.type,
        guardNeeded,
        swapped,
        swappedPairInfo: swappedPairInfo || null,
        durationExceeded:  durationExceeded  || false,
        deviationExceeded: deviationExceeded  || false,
        detourRatio:       detourRatio ?? null,
        isSpecialNeedsRoute: false,
        isNMTRoute: false,
        afterFleetExhaustion: afe || false,
        error: false,
      };

      applyPickupTimes(route, shiftTime, pickupTimePerEmployee, reportingTime, city);
      regularRoutes.push(route);
    }
  }

  // ── Combine routes ──
  const allRoutes = [...specialRoutes, ...regularRoutes];

  // ── Recovery: guarantee no valid-coordinate employee is left unrouted ──
  // No-op (returns immediately) when nothing was dropped — same output and
  // ~zero added time as before. Only fires for employees the solver couldn't
  // place (capacity/duration infeasibility) or whole groups that errored out.
  let finalUnrouted = unrouted;
  if (unrouted.length) {
    finalUnrouted = await recoverUnroutedEmployees({
      unrouted, allRoutes, facility, facilityCoord, isDropoff, city, shiftTime,
      pickupTimePerEmployee, reportingTime, maxDuration, cdcRules, activateGuard,
      tripType, fleet, fleetUsed,
    });
  }

  // ── Number all routes ──
  allRoutes.forEach((r, i) => { r.routeNumber = i + 1; });

  const totalRouted = new Set(allRoutes.flatMap(r => r.employees.map(e => e.empCode))).size;
  console.log(`[v3] ─── Done: ${allRoutes.length} routes | ${totalRouted} routed | ${finalUnrouted.length} unrouted ───`);

  return buildResponse({ data, tripType, allEmps, allRoutes, unrouted: finalUnrouted, fleet, fleetUsed, shiftTime, date });
}

// ─── response builder ─────────────────────────────────────────────────────────

function buildResponse({ data, tripType, allEmps, allRoutes, unrouted, fleet, fleetUsed, shiftTime, date }) {
  const isDropoff = tripType === 'DROPOFF';
  const routedCodes = new Set(allRoutes.flatMap(r => r.employees.map(e => e.empCode)));
  const totalOccupancy = allRoutes.reduce((s, r) => s + r.employees.length, 0);
  const facilityCoord = [data.facility?.geoY, data.facility?.geoX];

  return {
    uuid: data.uuid || uuidv4(),
    date,
    shift: shiftTime,
    tripType: isDropoff ? 'D' : 'P',
    totalEmployees: allEmps.length,
    totalRoutedEmployees: routedCodes.size,
    totalRoutes: allRoutes.length,
    totalGuardedRoutes: allRoutes.filter(r => r.guardNeeded).length,
    averageOccupancy: allRoutes.length
      ? parseFloat((totalOccupancy / allRoutes.length).toFixed(2))
      : 0,
    overallRouteDetails: {
      totalDistance: parseFloat(
        (allRoutes.reduce((s, r) => s + (r.routeDetails?.totalDistance || 0), 0) / 1000).toFixed(2)
      ),
      totalDuration: parseFloat(
        allRoutes.reduce((s, r) => s + (r.routeDetails?.totalDuration || 0), 0).toFixed(2)
      ),
    },
    totalSwappedRoutes: allRoutes.filter(r => r.swapped).length,

    routes: allRoutes.map(route => {
      const farthest = isDropoff
        ? route.employees[route.employees.length - 1]
        : route.employees[0];
      const farthestDist = farthest && facilityCoord[0]
        ? parseFloat(haversineKm(
            [farthest.location.lat, farthest.location.lng],
            facilityCoord
          ).toFixed(2))
        : 0;

      return {
        routeNumber:         route.routeNumber,
        zone:                route.zone,
        vehicleCapacity:     route.vehicleCapacity,
        vehicleType:         route.assignedVehicleType,
        guard:               route.guardNeeded   || false,
        swapped:             route.swapped        || false,
        swappedPairInfo:     route.swappedPairInfo || null,
        durationExceeded:    route.durationExceeded  || false,
        deviationExceeded:   route.deviationExceeded || false,
        detourRatio:         route.detourRatio ?? null,
        uniqueKey:           route.uniqueKey,
        isSpecialNeedsRoute: route.isSpecialNeedsRoute || false,
        afterFleetExhaustion: route.afterFleetExhaustion || false,
        distance:  parseFloat(((route.routeDetails?.totalDistance || 0) / 1000).toFixed(2)),
        duration:  parseFloat((route.routeDetails?.totalDuration || 0).toFixed(2)),
        occupancy: route.employees.length,
        farthestEmployeeDistance: farthestDist,
        isMedicalRoute: route.employees.some(e => e.isMedical),
        isPWDRoute:     route.employees.some(e => e.isPWD),
        isNMTRoute:     route.isNMTRoute || route.employees.some(e => e.isNMT),
        isOOBRoute:     route.employees.some(e => e.isOOB),
        encodedPolyline: route.encodedPolyline || 'no_polyline',
        employees: route.employees.map((emp, idx) => ({
          empCode:   emp.empCode,
          gender:    emp.gender,
          isMedical: emp.isMedical || false,
          isPWD:     emp.isPWD     || false,
          isNMT:     emp.isNMT     || false,
          isOOB:     emp.isOOB     || false,
          eta:       isDropoff ? emp.dropoffTime : emp.pickupTime,
          order:     emp.order ?? idx + 1,
          geoX:      emp.geoX,
          geoY:      emp.geoY,
        })),
      };
    }),

    unroutedEmployees: unrouted.map(emp => ({
      empCode:   emp.empCode,
      geoX:      emp.geoX,
      geoY:      emp.geoY,
      gender:    emp.gender,
      isMedical: emp.isMedical || false,
      isPWD:     emp.isPWD     || false,
      isNMT:     emp.isNMT     || false,
      isOOB:     emp.isOOB     || false,
    })),

    fleetUtilization: {
      totalVehiclesUsed:      allRoutes.length,
      totalVehiclesAvailable: fleet.reduce((s, v) => s + v.count, 0),
      byType: fleet.map(v => ({
        type:        v.type,
        capacity:    v.capacity,
        total:       v.count,
        used:        fleetUsed[v.type] || 0,
        remaining:   Math.max(0, v.count - (fleetUsed[v.type] || 0)),
        utilization: parseFloat(
          (((fleetUsed[v.type] || 0) / v.count) * 100).toFixed(1)
        ),
      })),
    },
  };
}

module.exports = { generateRoutes };
