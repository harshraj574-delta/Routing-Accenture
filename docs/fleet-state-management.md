# Live Fleet Management System — Design

**Status:** Draft v1
**Date:** 2026-06-17
**Goal:** Replace the static fleet (`profile.fleet: [{type,capacity,count}]`) with a
**live, stateful fleet** — every cab is an entity with status, position, driver, and
duty/availability — like MoveInSync / Routematic. The routing engine *queries live
availability* instead of being handed a constant.

---

## 1. The core shift

### Today (fleet-as-config)
```
Route request ──► routeGenerationService_v3
                     fleet     = profile.fleet            // static [{type,capacity,count}]
                     fleetUsed = {}                        // counter, reset every solve
                     pickVehicle(occupancy, fleet, used)   // smallest type w/ count left
```
The fleet is a **number per type**. No identity, no state, no time, no reuse across solves.

### Target (fleet-as-stateful-service)
```
                 ┌─────────────────────────────────────────────┐
                 │  Fleet Management Service (new)              │
                 │  • Vehicle registry (identity, type, vendor) │
                 │  • Live state store (status, pos, driver)    │
                 │  • Duty/shift + availability windows         │
                 │  • Allocation (reserve / release)            │
                 └───────────────┬─────────────────────────────┘
                                 │ GET /fleet/availability?city&facility&window&type
                                 │   → [{type, capacity, count}]   ← SAME SHAPE the solver eats
                                 ▼
Route request ──► routeGenerationService_v3  (unchanged interface)
                                 │ POST /fleet/allocate   (reserve specific vehicleIds for the plan)
                                 ▼
                     each route now carries a real vehicleId + driverId
```

**The seam is tiny.** The solver still receives `[{type,capacity,count}]`. We only change
who produces it (live service vs. static profile) and, after solving, we *bind* each route
to a concrete vehicle and flip its state to busy.

---

## 2. Vehicle state machine (the heart of it)

Every vehicle is always in exactly one state. Transitions are driven by events
(driver app, ops console, scheduler).

```
        driver login + duty-start
 OFFLINE ───────────────────────────►  AVAILABLE  ◄────────────────┐
   ▲                                    (in pool,                   │
   │ duty-end / 8h on-wheels /          idle, on-duty)              │ trip complete &
   │ 6 trips reached                        │                       │ within duty hrs &
   │                                        │ allocated to a route   │ rotation < max
 OFF_DUTY ◄──────────────────────┐         ▼                        │
                                  │     ASSIGNED (reserved)          │
                                  │         │ driver starts trip     │
                                  │         ▼                        │
                                  │   EN_ROUTE_PICKUP                │
                                  │         │ first pickup reached   │
                                  │         ▼                        │
                                  └──── ON_TRIP ──────────────────────┘
                                            │ (drop trip) reached office
                                            ▼
                                    RETURNING_TO_OFFICE
                                            │ available for a pickup on the way back  → AVAILABLE
                                            ▼  (constraint #12)

 Side states (from AVAILABLE/ON_TRIP): BREAKDOWN, MAINTENANCE, SUSPENDED (doc expired)
```

- **Rotation counter** per vehicle per duty (constraint #11: max 5–6 trips).
- **On-wheels timer** per duty (constraint #9: 8 h). When exceeded → forced OFF_DUTY,
  removed from availability.
- **RETURNING_TO_OFFICE → AVAILABLE for inbound pickup** is exactly constraint #12.

---

## 3. Domain model

**Master data (SQL Server — durable):**
- `vehicle` — id, regNo, type(s/m/l), capacity, ownership(owned/vendor), vendorId, city,
  facilityId(s), active, documents(RC/insurance/PUC/permit expiry).
- `driver` — id, name, licenseNo, docs+expiry, policeVerificationExpiry, phone, rating,
  homeCity.
- `vendor` — id, name, SLA, vehicle pool.
- `duty_roster` — vehicleId, driverId, city, shiftWindow(start,end), date. Drives which
  vehicles are *eligible* to be available in a given time window (constraint #3 / #5).

**Live state (Redis — hot, ephemeral, sub-ms):**
- `veh:state:{vehicleId}` → `{status, driverId, lat, lng, lastPingTs, currentTripId,
  tripsToday, onWheelsSec, availableFromTs, availableNearLat/Lng}`
- `veh:pool:{city}:{facility}:{type}` → Redis SET of vehicleIds currently AVAILABLE
  (status index for O(1) availability counts).
- `veh:reserved:{tripId}` → vehicleId (allocation lock, TTL until trip starts).

> Why split: master data changes rarely (SQL); live state changes every few seconds and is
> queried constantly (Redis). Never put GPS/status churn in SQL Server (see arch doc, 400+
> writes/sec at 2000 vehicles).

---

## 4. The two APIs the router uses

### A. Availability query (read) — replaces `profile.fleet`
```
GET /fleet/availability?city=BLR&facility=BDC&window=2030-0700&types=s,m,l
→ {
    "fleet": [
      { "type": "s", "capacity": 4, "count": 132 },   // live AVAILABLE in window
      { "type": "m", "capacity": 5, "count": 870 },
      { "type": "l", "capacity": 8, "count": 168 }     // 12-seater: only the 168 on-duty
    ],                                                 //   in the 0830-0730 window (#3)
    "asOf": "2026-06-17T20:15:00+05:30"
  }
```
Drop-in: `const fleet = liveFleet?.fleet?.length ? liveFleet.fleet : profile.fleet;`
The solver code at `routeGenerationService_v3.js:1190` is the only line that changes.

### B. Allocation (write) — after solving, bind plans to real cabs
```
POST /fleet/allocate
{ "tripId": "...", "city":"BLR", "facility":"BDC",
  "routes": [ {"routeId":"R1","type":"m","seats":5,"firstPickupEta":"...","area":"..."},
              {"routeId":"R2","type":"s","seats":3, ...} ] }
→ { "assignments": [ {"routeId":"R1","vehicleId":"KA01AB1234","driverId":"D88"},
                     {"routeId":"R2","vehicleId":"KA01XY9","driverId":"D12"} ],
    "unfulfilled": [] }            // flips each vehicle AVAILABLE → ASSIGNED, reserves it
```
Allocation picks *which* concrete cab (nearest idle one of that type, vendor-balanced,
docs-valid), not just a count. Release happens on trip completion → back to AVAILABLE.

---

## 5. How this solves the three hard email constraints

| Constraint | Solved by |
|---|---|
| **#3 time-windowed fleet availability** (12-seater windows 173/171/168) | `duty_roster` defines which vehicles are on-duty per window; availability query returns only those. The static `count` becomes a live, window-aware count. |
| **#11 max 5–6 trips/vehicle** | `tripsToday` counter on `veh:state`; vehicle drops out of the pool when it hits the cap. |
| **#9 driver 8h on-wheels** | `onWheelsSec` accumulator; forced OFF_DUTY past the limit. |
| **#12 drop→pickup chaining** | On RETURNING_TO_OFFICE, vehicle re-enters availability with `availableFromTs`/`availableNearLat,Lng`, so the next solve (or a continuous scheduler) can chain an inbound pickup onto it. |

These four are *impossible to express in a static fleet array* — which is exactly why
they're the current gaps. A stateful fleet makes them natural.

---

## 6. Event-driven transitions (fed by your existing driver app)

Your driver app already emits live status + GPS. Formalize those into fleet events:

| Event (from driver app / ops) | Transition |
|---|---|
| `driver.login` + `duty.start` | OFFLINE → AVAILABLE; add to pool |
| `trip.assigned` (from allocation) | AVAILABLE → ASSIGNED; remove from pool |
| `trip.started` | ASSIGNED → EN_ROUTE_PICKUP |
| `pickup.firstReached` | EN_ROUTE_PICKUP → ON_TRIP |
| `trip.completed` (drop) | ON_TRIP → RETURNING / AVAILABLE; `tripsToday++` |
| `gps.ping` | update pos; recompute `onWheelsSec` |
| `breakdown` / `maintenance` | → BREAKDOWN / MAINTENANCE; remove from pool |
| `duty.end` or onWheels ≥ 8h | → OFF_DUTY → OFFLINE |

This is the same event stream as the real-time plane in
`fleet-management-architecture.md` — the fleet-state store is just another consumer of it.

---

## 7. Build phases (start small, ship the foundation first)

1. **Registry + state store (foundation).** `vehicle`/`driver`/`vendor`/`duty_roster`
   tables (SQL) + Redis live-state keys + the vehicle state machine as a small service.
   Seed it from the BDC/DDC fleet counts in the email so it produces today's numbers.
2. **Availability API + router integration.** Implement `GET /fleet/availability`; switch
   `routeGenerationService_v3.js:1190` to consume it. Now routing runs on live counts.
   *(Solves #3 immediately.)*
3. **Allocation API + state flips.** `POST /fleet/allocate`; bind routes to real vehicleIds;
   release on completion. Now you have true busy/available tracking.
4. **Driver-app event wiring.** Connect login/duty/trip/gps events to transitions.
   On-wheels + rotation counters. *(Solves #9, #11.)*
5. **Multi-trip / drop→pickup chaining.** Use `availableFromTs`/location to chain inbound
   pickups. *(Solves #12.)* This is also where the routing engine grows multi-trip support.
6. **Ops fleet console.** Live grid/map of every cab by status (available/busy/breakdown),
   manual reassignment — the MoveInSync "fleet view".

**Recommendation:** Phases 1–2 are the unlock and are small (the router seam is one line).
They convert "we send a constant fleet" into "routing runs on a live fleet" — the exact
thing you described — without touching the solver internals.

---

## 8. Where this sits vs. the other docs
- `fleet-management-architecture.md` — the whole real-time platform (3 planes).
- `etms-feature-matrix-mapping.md` — procurement scoring across all modules.
- **This doc** — the *fleet-state core*: the single source of truth for "what cab is free,
  where, and for how long." It is the dependency that route-allocation, live tracking, and
  the ops console all build on. **Build this first.**
