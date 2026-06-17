"""
OR-Tools VRP Solver v2

Receives a JSON payload on stdin, solves the CVRP (Capacitated Vehicle
Routing Problem) with a direction-aware cost function, and writes the
solution as JSON to stdout.

Key improvements over v1:
  - Accepts solver_time_limit_seconds from the caller (scales with problem size)
  - Fixed dropped-node detection (set-based, not fragile routing-loop check)
  - Cleaner solution extraction loop
  - Better first-solution strategy for large instances (PATH_CHEAPEST_ARC)
"""

import sys
import json
import math
from ortools.constraint_solver import routing_enums_pb2, pywrapcp


def _err(msg):
    print(f"[PYTHON_ERR] {msg}", file=sys.stderr, flush=True)

def _dbg(msg):
    print(f"[PYTHON_DBG] {msg}", file=sys.stderr, flush=True)


# ─── data model ───────────────────────────────────────────────────────────────

def create_data_model(inp):
    BIG = 999_999_999

    def to_int_matrix(raw):
        return [
            [int(round(v)) if v is not None else BIG for v in row]
            for row in raw
        ]

    data = {}
    data["distance_matrix"] = to_int_matrix(inp["distance_matrix"])
    data["duration_matrix"] = to_int_matrix(
        inp.get("duration_matrix", inp["distance_matrix"])
    )
    data["demands"]           = [int(d) for d in inp["demands"]]
    data["vehicle_capacities"]= [int(c) for c in inp["vehicle_capacities"]]
    data["num_vehicles"]      = int(inp["num_vehicles"])
    data["depot"]             = int(inp.get("depot_index", 0))

    raw_dur = inp.get("max_route_duration")
    data["max_route_duration"] = int(round(raw_dur)) if raw_dur is not None else None

    st = inp.get("service_times", [0] * len(data["demands"]))
    data["service_times"] = [int(round(v)) if v is not None else 0 for v in st]

    data["allow_dropping_visits"] = bool(inp.get("allow_dropping_visits", False))
    data["drop_visit_penalty"]    = int(inp.get("drop_visit_penalty", 5_000_000))
    data["trip_type"]             = inp.get("trip_type", "PICKUP").upper()
    data["direction_penalty_weight"] = float(inp.get("direction_penalty_weight", 1.0))

    # Cross-track (lateral) penalty inputs: per-node bearing from the depot in
    # degrees and straight-line depth from the depot in metres. Optional — when
    # absent or weight is 0 the lateral term is disabled.
    data["lateral_penalty_weight"] = float(inp.get("lateral_penalty_weight", 0.0))
    data["node_bearings_deg"] = inp.get("node_bearings_deg") or None
    data["node_depths_m"]     = inp.get("node_depths_m") or None

    # Time limit: caller can override, otherwise we scale with problem size
    n = len(data["distance_matrix"])
    default_limit = (
        20 if n <= 31 else
        30 if n <= 61 else
        40 if n <= 101 else
        55 if n <= 201 else
        70
    )
    data["solver_time_limit_seconds"] = int(
        inp.get("solver_time_limit_seconds", default_limit)
    )

    # Validation
    n = len(data["distance_matrix"])
    if n > 0:
        if not all(len(r) == n for r in data["distance_matrix"]):
            raise ValueError("distance_matrix is not square")
        if len(data["demands"]) != n:
            raise ValueError("demands length mismatch")
        if len(data["service_times"]) != n:
            raise ValueError("service_times length mismatch")
    if data["num_vehicles"] > 0 and len(data["vehicle_capacities"]) != data["num_vehicles"]:
        raise ValueError("vehicle_capacities length mismatch")

    _dbg(f"Model: {n} locations, {data['num_vehicles']} vehicles, "
         f"depot={data['depot']}, trip={data['trip_type']}, "
         f"time_limit={data['solver_time_limit_seconds']}s")
    return data


# ─── solver ───────────────────────────────────────────────────────────────────

def solve(data):
    n = len(data["distance_matrix"])
    depot = data["depot"]

    if n == 0:
        return {"routes": [], "dropped_node_indices": []}
    if data["num_vehicles"] == 0 and n > 1:
        return {"routes": [], "dropped_node_indices": list(range(1, n))}

    manager = pywrapcp.RoutingIndexManager(n, data["num_vehicles"], depot)
    routing  = pywrapcp.RoutingModel(manager)

    # ── Cost callback: travel distance + direction penalty + lateral penalty ──
    # Direction term — for PICKUP penalise moving further away from depot
    # (increasing dist-to-depot); for DROPOFF penalise moving closer. Bakes
    # monotonic (no-backtracking) routes directly into the solver objective.
    # Lateral term — penalise the CROSS-TRACK component of each arc (sideways
    # movement perpendicular to the depot direction). Without it the solver can
    # snake side-to-side across a sector at constant depth with zero penalty:
    # routes stay monotonic in depth yet look zigzaggy on the map. Cross-track
    # distance is approximated as arc length: |Δbearing| × mean depth.
    penalty_weight = data["direction_penalty_weight"]
    lateral_weight = data["lateral_penalty_weight"]
    bearings       = data["node_bearings_deg"]
    depths         = data["node_depths_m"]
    trip_type      = data["trip_type"]
    dist_matrix    = data["distance_matrix"]

    use_lateral = bool(
        lateral_weight > 0
        and bearings and len(bearings) == n
        and depths   and len(depths)   == n
    )

    def cost_callback(from_idx, to_idx):
        from_node = manager.IndexToNode(from_idx)
        to_node   = manager.IndexToNode(to_idx)

        travel = dist_matrix[from_node][to_node]
        penalty = 0

        if from_node != depot and to_node != depot:
            if penalty_weight > 0:
                dist_from_to_depot = dist_matrix[from_node][depot]
                dist_to_to_depot   = dist_matrix[to_node][depot]

                if trip_type == "PICKUP":
                    # Moving away from depot is bad (pickup order should converge on depot)
                    increase = dist_to_to_depot - dist_from_to_depot
                    if increase > 0:
                        penalty += int(penalty_weight * increase)
                else:
                    # DROPOFF: moving back towards depot is bad (should go outward)
                    decrease = dist_from_to_depot - dist_to_to_depot
                    if decrease > 0:
                        penalty += int(penalty_weight * decrease)

            if use_lateral:
                diff = abs(bearings[from_node] - bearings[to_node]) % 360.0
                if diff > 180.0:
                    diff = 360.0 - diff
                cross_track_m = math.radians(diff) * (depths[from_node] + depths[to_node]) / 2.0
                penalty += int(lateral_weight * cross_track_m)

        return travel + penalty

    cost_cb_idx = routing.RegisterTransitCallback(cost_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(cost_cb_idx)

    # ── Capacity dimension ────────────────────────────────────────────────────
    def demand_callback(from_idx):
        return data["demands"][manager.IndexToNode(from_idx)]

    demand_cb_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_cb_idx, 0, data["vehicle_capacities"], True, "Capacity"
    )

    # ── Max route duration dimension ──────────────────────────────────────────
    if data["max_route_duration"] is not None:
        dur_matrix = data["duration_matrix"]
        svc_times  = data["service_times"]

        def duration_callback(from_idx, to_idx):
            from_node = manager.IndexToNode(from_idx)
            to_node   = manager.IndexToNode(to_idx)
            return svc_times[from_node] + dur_matrix[from_node][to_node]

        dur_cb_idx = routing.RegisterTransitCallback(duration_callback)
        routing.AddDimension(
            dur_cb_idx, 0, data["max_route_duration"], False, "Duration"
        )

    # ── Allow dropping visits ─────────────────────────────────────────────────
    if data["allow_dropping_visits"]:
        pen = data["drop_visit_penalty"]
        for node in range(n):
            if node == depot:
                continue
            routing.AddDisjunction([manager.NodeToIndex(node)], pen)

    # ── Search parameters ─────────────────────────────────────────────────────
    params = pywrapcp.DefaultRoutingSearchParameters()

    # PATH_CHEAPEST_ARC scales much better than AUTOMATIC for large instances
    params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    params.time_limit.FromSeconds(data["solver_time_limit_seconds"])
    params.log_search = False

    _dbg("Solving...")
    solution = routing.SolveWithParameters(params)

    status_int = routing.status()
    try:
        status_name = routing_enums_pb2.RoutingSearchStatus.DESCRIPTOR \
            .values_by_number[status_int].name
    except Exception:
        status_name = str(status_int)
    _dbg(f"Status: {status_name}")

    # ── Extract solution ──────────────────────────────────────────────────────
    routes_out = []
    routed_nodes = set()

    if solution:
        for vid in range(data["num_vehicles"]):
            idx   = routing.Start(vid)
            nodes = []
            while not routing.IsEnd(idx):
                node = manager.IndexToNode(idx)
                idx  = solution.Value(routing.NextVar(idx))
                if node != depot:
                    nodes.append(node)
                    routed_nodes.add(node)
            if nodes:
                routes_out.append({"vehicle_index": vid, "node_indices": nodes})
    else:
        _err("No solution found")

    # Dropped = all non-depot nodes not present in any route
    dropped = [
        i for i in range(n)
        if i != depot and i not in routed_nodes
    ]

    _dbg(f"Routes: {len(routes_out)}, Dropped: {len(dropped)}")
    return {"routes": routes_out, "dropped_node_indices": dropped}


# ─── entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    raw = sys.stdin.read()
    if not raw.strip():
        print(json.dumps({"error": "No input", "routes": [], "dropped_node_indices": []}))
        sys.exit(1)

    try:
        inp  = json.loads(raw)
        data = create_data_model(inp)
        result = solve(data)
        print(json.dumps(result))
    except Exception as e:
        import traceback
        _err(traceback.format_exc())
        print(json.dumps({"error": str(e), "routes": [], "dropped_node_indices": []}))
        sys.exit(1)
