# Fleet Management Platform — Architecture & Design

**Status:** Draft v1
**Author:** Engineering
**Date:** 2026-06-17
**Context:** Evolve the existing batch routing engine (OR-Tools VRP + Express + SQL Server)
into a production-grade, real-time fleet management & tracking platform, integrated with
the routing engine so plans react to live ground truth.

---

## 0. Design constraints (decided)

| Constraint | Value | Architectural consequence |
|---|---|---|
| Target scale | **2000+ concurrent vehicles** pinging | Real streaming backbone (Kafka / NATS JetStream), partitioned ingest, horizontally scaled socket + worker tiers. A single-node Redis-only design will not hold. |
| GPS source | **Driver mobile app only** (phone GPS) | Ingest over WebSocket/HTTPS, not MQTT device fleets. No OBD/CAN data (fuel, engine, hard-braking from ECU). No hardware device-management plane. Location + app-reported events only. |
| Existing assets | OR-Tools VRP, versioned Express API, async job store (SQL Server), Google ETA | Reused as the **Planning service**. Not rewritten. |
| Existing apps | Driver app + employee app with live trip tracking | Driver app becomes the formal GPS *producer*; employee app becomes a real-time *consumer*. |

At 2000+ vehicles × 1 ping / 5s ≈ **400 writes/sec sustained**, with bursts at shift
start/end. That is trivial for a broker, fatal for SQL Server if written synchronously.
The whole design follows from that single number.

---

## 1. The three planes

Keep these decoupled. Do **not** make the planner real-time.

```
 PLANNING PLANE                REAL-TIME PLANE                 OPERATIONS PLANE
 (batch, minutes)              (streaming, 1-10s)              (live + human-in-loop)
 ┌──────────────┐             ┌────────────────────┐          ┌────────────────────┐
 │ OR-Tools VRP │◄── re-opt ──│ ingest → broker →  │── push ─►│ Ops dashboard      │
 │ + job store  │   trigger   │ stream processors  │          │ (live map, alerts) │
 │ (EXISTING)   │── matrix ──►│ + hot/cold stores  │          │ Employee app feed  │
 └──────────────┘  (travel    └────────────────────┘          └────────────────────┘
                    times)
```

- **Planning** answers *"what is the optimal plan?"* — your existing engine.
- **Real-time** answers *"where is everything right now, and is it on plan?"*
- **Operations** answers *"what is deviating, and what do we do about it?"*

---

## 2. High-level architecture

```
                                  Driver App (producer)
                                  │  WSS: {tripId, vehicleId, lat, lng, speed,
                                  │        heading, accuracy, ts, batteryPct}
                                  ▼
                        ┌───────────────────────────┐
                        │  Ingest Gateway (N pods)   │  stateless, auth, validate,
                        │  WebSocket + HTTP fallback │  rate-limit, batch
                        └───────────────┬───────────┘
                                        │ produce (key = vehicleId)
                                        ▼
                        ┌───────────────────────────┐
                        │  Broker: Kafka / NATS JS   │  topic: gps.raw  (partitioned by vehicleId)
                        └───────────────┬───────────┘
                 ┌──────────────────────┼───────────────────────┐
                 ▼                      ▼                         ▼
        ┌─────────────────┐   ┌──────────────────┐    ┌────────────────────┐
        │ Position worker │   │ Enrichment worker│    │ Geofence/ETA worker │
        │ → Redis hot pos │   │ map-match, clean │    │ events, off-route,  │
        │ → TS cold trail │   │ → gps.clean topic│    │ ETA → events topic  │
        └────────┬────────┘   └──────────────────┘    └─────────┬──────────┘
                 │                                               │
                 ▼                                               ▼
        ┌─────────────────┐                            ┌────────────────────┐
        │ Redis           │                            │ events topic        │
        │ pos:vehicle:*   │                            │ (trip_started, sos, │
        │ (latest, TTL)   │                            │  off_route, delay…) │
        └────────┬────────┘                            └─────────┬──────────┘
                 │                                                │
                 └──────────────┬─────────────────────────────────┘
                                ▼
                  ┌──────────────────────────┐
                  │ Realtime Fanout (Socket.IO│  rooms: trip:*, vehicle:*, fleet:*
                  │ cluster + Redis adapter)  │  push to dashboards & employee app
                  └────────────┬─────────────┘
                               │
              ┌────────────────┴──────────────────┐
              ▼                                    ▼
   ┌────────────────────┐                ┌──────────────────────┐
   │ Ops Dashboard       │                │ Employee app          │
   │ (React + Mapbox GL) │                │ (live driver dot/ETA) │
   └────────────────────┘                └──────────────────────┘

   Side stores:
   - SQL Server (EXISTING): business entities, bookings, jobs, billing
   - TimescaleDB / PostGIS:  location history (cold), trip execution facts
```

### Why this shape

- **Ingest is dumb & stateless.** It authenticates, validates, batches, and produces to
  the broker. It never touches SQL. Scales horizontally behind a load balancer.
- **Broker absorbs the firehose** and decouples producers from consumers. Partition by
  `vehicleId` so all of one vehicle's pings stay ordered on one partition.
- **Hot/cold split.** Redis answers *"where is vehicle 42 now?"* in sub-ms. Time-series DB
  answers *"trail of trip X."* SQL Server is never in the GPS write path.
- **Fanout tier is separate from ingest** so a dashboard reconnect storm can't degrade
  ingestion, and vice versa.

---

## 3. Components in detail

### 3.1 Ingest Gateway
- Protocol: **WebSocket (WSS)** primary; HTTPS POST batch fallback for poor networks.
- Driver app sends a position every 5s while a trip is `live` (back off to 30s when idle/
  stationary to save battery & bandwidth — *adaptive ping rate* is critical at 2000+ phones).
- Responsibilities: JWT auth (driver identity), schema validation, dedupe, server-side
  timestamp, **micro-batching** (buffer 1–2s, produce in batches), backpressure.
- Stateless → autoscale on connection count / CPU.

### 3.2 Broker
- **Kafka** (managed: MSK / Confluent / Redpanda) or **NATS JetStream** (lighter ops).
  At 2000+ vehicles with growth headroom and replay needs, this is justified.
- Topics: `gps.raw`, `gps.clean`, `fleet.events`. Partition by `vehicleId`.
- Retention: `gps.raw` short (hours); `fleet.events` longer (replayable audit log).

### 3.3 Stream processors (workers, independently scalable)
1. **Position worker** — writes latest position to Redis (`pos:vehicle:{id}`, TTL ~60s so
   stale vehicles disappear) and appends to the time-series cold store.
2. **Enrichment worker** — noise filtering + **map-matching** (snap to road via OSRM /
   Valhalla self-hosted, or Google Roads API) → emits `gps.clean`. Raw phone GPS jumps;
   never show or route on raw pings.
3. **Geofence / ETA worker** — detects campus/pickup enter-exit, off-route deviation,
   idle, speeding (from GPS-derived speed), delay vs. plan; computes live ETA from current
   position to remaining stops; emits to `fleet.events`.

### 3.4 Stores
| Store | Purpose | Tech |
|---|---|---|
| **Hot positions** | "where now" | Redis (key-value, TTL) |
| **Cold trail / facts** | history, breadcrumb replay, analytics | **TimescaleDB** (Postgres + time-series) with **PostGIS** for geo queries |
| **Event log** | immutable trip events, audit | broker (`fleet.events`) + projection into Postgres |
| **Business entities** | vehicles, drivers, vendors, bookings, jobs, billing | **SQL Server (existing)** |

> Decision: add Postgres/TimescaleDB+PostGIS as the geo/time-series store rather than
> bending SQL Server to it. Geospatial + time-series workloads are exactly where Postgis/
> Timescale excel and SQL Server struggles operationally at this write rate.

### 3.5 Realtime fanout
- **Socket.IO cluster** with the **Redis adapter** (so any pod can push to any client).
  Rooms: `trip:{id}` (employee app subscribes to their trip), `vehicle:{id}`,
  `fleet:{tenant}` (dispatcher sees all). Alternative if you'd rather not run this tier:
  managed **Ably / Pusher / AWS AppSync**.
- Clients receive position + event deltas, never poll.

### 3.6 Ops dashboard (new frontend)
- **React + Mapbox GL JS** (or MapLibre to avoid Mapbox billing) with **deck.gl** for
  rendering thousands of moving markers efficiently (WebGL, not DOM markers).
- Live map (status-colored vehicles), alert feed, trip drill-down with plan-vs-actual
  overlay, ETA panel, occupancy, SOS handling, replay (from cold store).
- Subscribes to Socket.IO; reads history via a query API over TimescaleDB.

---

## 4. Closing the loop: real-time data → better routes

Three levels, ship in order.

### Level 1 — Time-dependent travel times (highest ROI, lowest risk)
Feed the VRP **traffic-aware, time-bucketed** travel times instead of static distances.
- Short term: Google Distance Matrix with `departure_time` + traffic model.
- Medium term: **learn your own speed profiles** from the cold GPS store (per road segment,
  per time-of-day, per day-of-week). Your own fleet becomes your traffic data source — this
  is exactly what mature corporate-transport platforms do and removes Google cost/limits.
- Touches only the cost-matrix builder feeding OR-Tools. No architecture change.

### Level 2 — Dynamic re-optimization on disruption
On no-show / late driver / breakdown / late booking: trigger a **partial re-solve** of the
*remaining* stops. Lock completed/in-progress stops, re-optimize the tail. OR-Tools supports
this directly. **Your existing async job store is already the right mechanism** — disruption
events from `fleet.events` enqueue a re-opt job; result pushed back via the fanout tier.

### Level 3 — Continuous/online dispatch (Uber/Ola territory) — *out of scope*
Streaming request-vehicle matching. Employee transport is shift-based and plannable, so this
is explicitly **not** built unless on-demand ad-hoc rides become a product. Flagged to avoid
over-engineering.

---

## 5. Domain model (new + existing)

Planned route ≠ live trip. Keep them separate.

- **Vehicle** — capacity, type, ownership (owned/vendor), documents & expiry, status.
- **Driver** — license, compliance docs, shift, rating, current assignment, app session.
- **Vendor/Supplier** — most corporate fleets are mixed owned + outsourced; model vendors,
  SLAs, vehicle pools.
- **Route plan** — output of the VRP (ordered stops, manifest, planned ETAs). *Immutable plan.*
- **Trip (execution)** — the live run of a plan: actual positions, actual stop times, status,
  deviations. *This is the object the real-time plane owns.*
- **Booking / passenger manifest** — riders, pickup/drop points, special needs.
- **Geofence** — campus, pickup zones, restricted areas.
- **Event** — immutable: `trip_started`, `stop_reached`, `passenger_boarded`, `off_route`,
  `delay`, `sos`, `idle`, `trip_completed`.
- **Shift** — login/logout windows that drive planning.
- *(Phase 5)* Maintenance, fuel, compliance, billing/invoicing.

---

## 6. How the established players do it (reference)

- **Samsara / Geotab** — hardware-first telematics: device → MQTT ingest → stream processing
  → time-series store → live dashboards. (We skip the device plane: phone-only.)
- **MoveInSync / Routematic / WeGo** (corporate employee transport — closest analog) —
  shift-based batch planning + live tracking + geofenced auto trip-state + SOS/compliance +
  vendor management. Level 1+2 routing loop, not continuous dispatch. **This is our template.**
- **Uber / Ola dispatch** — continuous online matching, supply-demand prediction, surge.
  Relevant only if on-demand becomes a product (Level 3).
- **Onfleet / Routific** — last-mile delivery: plan + live ETA + proof-of-delivery + re-opt.

Common thread: **batch planner kept separate from a streaming telematics pipeline**, joined
by (a) feeding live travel times into planning and (b) event-triggered re-optimization.

---

## 7. Rollout phases

| Phase | Deliverable | Value | Risk |
|---|---|---|---|
| **1. Visibility** | Ingest → broker → Redis → Socket.IO → live map dashboard | See the fleet live | Low |
| **2. Domain + ops** | Vehicle/driver/vendor/trip-execution models, geofence events, alerts (delay/off-route/SOS/idle) | Operational control | Med |
| **3. Smarter routing** | Time-dependent travel times in VRP (learned from Phase 1 history) | Routes genuinely better | Low-Med |
| **4. Dynamic re-opt** | Disruption events → partial re-solve via existing job engine | Plans react to reality | Med |
| **5. Fleet breadth** | Maintenance, fuel, compliance, billing modules | Full "management" suite | Med |

Phase 1 first: highest perceived value, lowest risk, and it generates the GPS history that
Phase 3 needs.

---

## 8. Concrete additions to the current stack

Existing Express + OR-Tools + SQL Server + async jobs are **kept and reused**. New pieces:

- **Driver app**: formalize the existing live-trip GPS feed into the ingest WSS contract;
  add adaptive ping rate (5s active / 30s idle) and offline buffering.
- **Backend deps**: `ioredis` (hot store + Socket.IO adapter), `socket.io`, a Kafka/NATS
  client (`kafkajs` / `nats`), a Postgres client + PostGIS/TimescaleDB.
- **New services** (separate deployables, not bolted into the routing API): `ingest-gateway`,
  `stream-workers` (position / enrichment / geofence-eta), `realtime-fanout`.
- **New frontend**: `ops-dashboard` (React + MapLibre/Mapbox GL + deck.gl).
- **Routing engine change** (Phase 3): swap the cost-matrix builder to time-dependent /
  learned travel times. (Phase 4): add a re-opt job type that locks fixed stops.

### Scaling notes for 2000+
- Adaptive + batched pings keep effective write rate manageable; don't ping stationary cabs at 5s.
- Partition broker by `vehicleId`; scale workers per-partition.
- Socket.IO must run clustered with Redis adapter — single node will not hold the dashboard +
  employee-app fan-out.
- Map-matching is the CPU-heavy step — self-host OSRM/Valhalla and scale that worker pool
  independently, or it becomes the bottleneck (and Google Roads API cost explodes).

---

## 9. Open questions / next decisions

1. **Broker**: Kafka (replay, ecosystem, heavier ops) vs. NATS JetStream (lighter, simpler)?
2. **Fanout**: self-host Socket.IO cluster vs. managed Ably/Pusher (faster, recurring cost)?
3. **Map-matching**: self-host OSRM/Valhalla (cost control at scale) vs. Google Roads (faster start)?
4. **Multi-tenancy**: is this single-client (Accenture) or a product for many clients? Changes
   isolation, data partitioning, and the dashboard's tenant model.
5. **Cloud**: current deploy is AWS App Runner — provision MSK/Redis/RDS-Postgres there, or
   go managed (Confluent/Upstash/Timescale Cloud)?
```
