import sys
import json
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import traceback # For detailed error logging
import math # For Haversine if needed

# --- Helper Functions ---
def print_debug(message):
    """Prints a debug message to stderr."""
    print(f"[PYTHON_DEBUG] {message}", file=sys.stderr, flush=True)

def print_error(message):
    """Prints an error message to stderr."""
    print(f"PYTHON_ERROR: {message}", file=sys.stderr, flush=True)

# Optional: Haversine function if matrix distances to depot aren't reliable/available
def haversine(lat1, lon1, lat2, lon2):
    """Calculate haversine distance between two points in METERS."""
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2)**2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# --- Data Model ---
def create_data_model(input_data):
    """Stores the data for the problem, ensuring integer types for OR-Tools."""
    print_debug("Creating data model...")
    data = {}
    # A large number to represent infinity or a very high penalty for unreachable nodes.
    # Ensure this is larger than any possible sum of actual distances/durations in a route.
    large_penalty_int = 999999999 # Used for None distances/durations

    try:
        data["distance_matrix"] = [
            [int(round(cost)) if cost is not None else large_penalty_int for cost in row]
            for row in input_data["distance_matrix"]
        ]
    except (TypeError, ValueError) as e:
        print_error(f"Error processing distance_matrix: {e}. Input was: {input_data.get('distance_matrix')}")
        raise

    raw_duration_matrix = input_data.get("duration_matrix", input_data["distance_matrix"])
    try:
        data["duration_matrix"] = [
            [int(round(cost)) if cost is not None else large_penalty_int for cost in row]
            for row in raw_duration_matrix
        ]
    except (TypeError, ValueError) as e:
        print_error(f"Error processing duration_matrix: {e}. Input was: {raw_duration_matrix}")
        raise

    data["demands"] = [int(d) for d in input_data["demands"]]
    data["vehicle_capacities"] = [int(c) for c in input_data["vehicle_capacities"]]
    data["num_vehicles"] = int(input_data["num_vehicles"])
    data["depot"] = int(input_data.get("depot_index", 0)) # Should be 0

    max_dur = input_data.get("max_route_duration", None)
    data["max_route_duration"] = int(round(max_dur)) if max_dur is not None else None

    service_times_input = input_data.get("service_times", [0] * len(data["demands"]))
    data["service_times"] = [int(round(st)) if st is not None else 0 for st in service_times_input]

    data["allow_dropping_visits"] = input_data.get("allow_dropping_visits", False)
    data["drop_visit_penalty"] = int(input_data.get("drop_visit_penalty", 5000000)) # Relative to cost units

    # --- NEW for Direction Penalty ---
    data["facility_coords"] = input_data.get("facility_coords") # [lat, lng]
    data["trip_type"] = input_data.get("trip_type", "PICKUP").upper()
    data["direction_penalty_weight"] = float(input_data.get("direction_penalty_weight", 1.0))

    # --- NEW for Re-optimization with Fixed Nodes ---
    data["fixed_start_node_index_in_matrix"] = input_data.get("fixed_start_node_index_in_matrix", None)
    data["fixed_end_node_index_in_matrix"] = input_data.get("fixed_end_node_index_in_matrix", None)
    # other_customer_node_indices_in_matrix: list of original indices for other customers in the route
    data["other_customer_node_indices_in_matrix"] = input_data.get("other_customer_node_indices_in_matrix", None)


    num_locations = len(data['distance_matrix'])
    print_debug(f"  Num locations (from dist_matrix): {num_locations}")
    print_debug(f"  Num vehicles: {data['num_vehicles']}")
    print_debug(f"  Depot index: {data['depot']}")
    print_debug(f"  Vehicle capacities: {data['vehicle_capacities']}")
    print_debug(f"  Demands: {data['demands']}")
    print_debug(f"  Max route duration: {data['max_route_duration']}")
    print_debug(f"  Service times: {data['service_times']}")
    print_debug(f"  Allow dropping visits: {data['allow_dropping_visits']}")
    print_debug(f"  Drop visit penalty: {data['drop_visit_penalty']}")
    print_debug(f"  Facility Coords: {data['facility_coords']}")
    print_debug(f"  Trip Type: {data['trip_type']}")
    print_debug(f"  Direction Penalty Weight: {data['direction_penalty_weight']}")
    print_debug(f"  Fixed Start Node Index (in matrix): {data['fixed_start_node_index_in_matrix']}")
    print_debug(f"  Fixed End Node Index (in matrix): {data['fixed_end_node_index_in_matrix']}")
    print_debug(f"  Other Customer Indices for Fixed End (in matrix): {data['other_customer_node_indices_in_matrix']}")


    # --- Validation ---
    if num_locations > 0:
        if not all(len(row) == num_locations for row in data['distance_matrix']):
            print_error("Distance matrix is not square or rows have inconsistent lengths.")
            raise ValueError("Distance matrix is not square.")
        if len(data['demands']) != num_locations:
            print_error(f"Demands length ({len(data['demands'])}) != num_locations ({num_locations}).")
            raise ValueError("Demands length mismatch.")
        if len(data['service_times']) != num_locations:
            print_error(f"Service times length ({len(data['service_times'])}) != num_locations ({num_locations}).")
            raise ValueError("Service times length mismatch.")
    if data["depot"] != 0:
         print_error(f"Depot index is assumed to be 0 for some logic (like direction penalty relative to matrix[node][0]), but received {data['depot']}. This might lead to incorrect behavior if not handled carefully everywhere.")
    if data["num_vehicles"] > 0 and len(data["vehicle_capacities"]) != data["num_vehicles"]:
        print_error(f"Mismatch: num_vehicles={data['num_vehicles']} but len(vehicle_capacities)={len(data['vehicle_capacities'])}")
        raise ValueError("Vehicle capacities length mismatch with num_vehicles.")

    # Validation for fixed node indices
    if data["fixed_start_node_index_in_matrix"] is not None:
        if not (0 <= data["fixed_start_node_index_in_matrix"] < num_locations):
            print_error(f"Invalid fixed_start_node_index_in_matrix: {data['fixed_start_node_index_in_matrix']} for num_locations {num_locations}")
            raise ValueError("Invalid fixed_start_node_index_in_matrix")
        if data["num_vehicles"] != 1:
            print_error(f"fixed_start_node_index_in_matrix is set, but num_vehicles is {data['num_vehicles']} (expected 1 for re-optimization).")
            # Not raising error, but logging, as Node.js should control this.
    if data["fixed_end_node_index_in_matrix"] is not None:
        if not (0 <= data["fixed_end_node_index_in_matrix"] < num_locations):
            print_error(f"Invalid fixed_end_node_index_in_matrix: {data['fixed_end_node_index_in_matrix']} for num_locations {num_locations}")
            raise ValueError("Invalid fixed_end_node_index_in_matrix")
        if data["num_vehicles"] != 1:
            print_error(f"fixed_end_node_index_in_matrix is set, but num_vehicles is {data['num_vehicles']} (expected 1 for re-optimization).")
        if data["other_customer_node_indices_in_matrix"] is not None:
            for idx in data["other_customer_node_indices_in_matrix"]:
                if not (0 <= idx < num_locations):
                    print_error(f"Invalid index {idx} in other_customer_node_indices_in_matrix for num_locations {num_locations}")
                    raise ValueError("Invalid index in other_customer_node_indices_in_matrix")
    # --- End Validation ---

    return data

# --- Solver ---
def solve_cvrptw(data_model):
    print_debug("Initializing OR-Tools Routing Manager and Model...")
    num_locations = len(data_model["distance_matrix"])
    depot_original_idx = data_model["depot"] # This is the original index (e.g., 0)

    # --- Basic checks ---
    if num_locations == 0:
        print_error("Cannot solve: distance_matrix is empty after data model creation.")
        return {"routes": [], "dropped_node_indices": [], "error": "Empty distance matrix in data model"}
    if data_model["num_vehicles"] == 0 and num_locations > 1: # depot + at least one customer
        print_error("Cannot solve: num_vehicles is 0 but there are locations to visit.")
        return {"routes": [], "dropped_node_indices": [], "error": "num_vehicles is 0 with locations to visit"}

    manager = pywrapcp.RoutingIndexManager(
        num_locations, data_model["num_vehicles"], depot_original_idx # Use original depot index here
    )
    routing = pywrapcp.RoutingModel(manager)
    print_debug("  Manager and Model created.")
    depot_manager_idx = manager.NodeToIndex(depot_original_idx)


    # --- *** MODIFIED COST CALLBACK *** ---
    def distance_with_direction_penalty_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)

        if not (0 <= from_node < num_locations and 0 <= to_node < num_locations):
            print_error(f"Modified Cost cb: Invalid node index. from_node={from_node}, to_node={to_node}, N={num_locations}")
            return data_model.get("large_penalty_int", 999999999)

        travel_distance = data_model["distance_matrix"][from_node][to_node]
        penalty = 0
        weight = data_model["direction_penalty_weight"]

        # Apply direction penalty only if not moving to/from depot and weight > 0
        # And only if facility_coords are available (needed for Haversine if matrix distances to depot are not used)
        # Current implementation uses matrix distances to depot.
        if from_node != depot_original_idx and to_node != depot_original_idx and weight > 0:
            try:
                # Using matrix distances to depot (index 0)
                dist_from_to_depot = data_model["distance_matrix"][from_node][depot_original_idx]
                dist_to_to_depot = data_model["distance_matrix"][to_node][depot_original_idx]

                if data_model["trip_type"] == "PICKUP":
                    # Penalize if moving further away from the depot
                    distance_increase_from_depot = dist_to_to_depot - dist_from_to_depot
                    if distance_increase_from_depot > 0:
                        penalty = weight * distance_increase_from_depot
                elif data_model["trip_type"] == "DROPOFF":
                    # Penalize if moving closer to the depot (unless it's the last leg for that customer)
                    # This logic is tricky. A simpler approach for dropoff might be to penalize
                    # if the *next stop* is closer to the depot than the *current stop*,
                    # but the current stop is not the furthest on the remaining path.
                    # For now, let's penalize if moving towards depot when not expected.
                    distance_decrease_to_depot = dist_from_to_depot - dist_to_to_depot # Positive if moving closer
                    if distance_decrease_to_depot > 0: # Moving closer to depot
                        penalty = weight * distance_decrease_to_depot
            except IndexError:
                 print_error(f"Modified Cost cb: IndexError accessing distance matrix for depot distances. from={from_node}, to={to_node}, depot_original_idx={depot_original_idx}")
            except Exception as e:
                 print_error(f"Modified Cost cb: Error calculating penalty: {e}")
        final_cost = travel_distance + penalty
        return int(round(final_cost))

    modified_cost_callback_index = routing.RegisterTransitCallback(
        distance_with_direction_penalty_callback
    )
    routing.SetArcCostEvaluatorOfAllVehicles(modified_cost_callback_index)
    print_debug(f"  Arc cost set to TOTAL DISTANCE + DIRECTION PENALTY (Weight: {data_model['direction_penalty_weight']}).")


    # --- *** ADD CONSTRAINTS FOR FIXED NODES (for re-optimization) *** ---
    # These constraints assume num_vehicles = 1 for re-optimization calls
    # Node.js should send allow_dropping_visits = False for re-optimization.
    vehicle_id_for_reopt = 0 # Assuming only one vehicle when these params are set

    fixed_start_node_orig_idx = data_model.get("fixed_start_node_index_in_matrix")
    fixed_end_node_orig_idx = data_model.get("fixed_end_node_index_in_matrix")

    if fixed_start_node_orig_idx is not None:
        # PICKUP: Depot -> fixed_start_node -> ... -> Depot
        if not (0 <= fixed_start_node_orig_idx < num_locations):
            print_error(f"Skipping fixed start constraint due to invalid index: {fixed_start_node_orig_idx}")
        else:
            fixed_start_node_manager_idx = manager.NodeToIndex(fixed_start_node_orig_idx)
            if fixed_start_node_manager_idx != -1 and depot_manager_idx != -1:
                print_debug(f"  Applying PICKUP constraint: Depot ({depot_original_idx}) -> Node {fixed_start_node_orig_idx} (Manager indices: {depot_manager_idx} -> {fixed_start_node_manager_idx}) for vehicle {vehicle_id_for_reopt}")
                # Force the vehicle to visit the fixed node immediately after the depot.
                routing.solver().Add(routing.NextVar(depot_manager_idx) == fixed_start_node_manager_idx)
                # Ensure the fixed node and depot are on this vehicle's route
                routing.solver().Add(routing.VehicleVar(fixed_start_node_manager_idx) == vehicle_id_for_reopt)
                routing.solver().Add(routing.VehicleVar(depot_manager_idx) == vehicle_id_for_reopt)
                # If allow_dropping_visits is False (expected for re-opt), nodes are mandatory by default.
                # If it were True, we'd need: routing.AddDisjunction([fixed_start_node_manager_idx], 0)
            else:
                print_error(f"Could not get manager index for fixed_start_node {fixed_start_node_orig_idx} or depot {depot_original_idx}")

    elif fixed_end_node_orig_idx is not None:
        # DROPOFF: Depot -> ...other_customers... -> fixed_end_node -> Depot
        other_customers_original_indices = data_model.get("other_customer_node_indices_in_matrix", [])
        if not (0 <= fixed_end_node_orig_idx < num_locations):
            print_error(f"Skipping fixed end constraint due to invalid index: {fixed_end_node_orig_idx}")
        else:
            fixed_end_node_manager_idx = manager.NodeToIndex(fixed_end_node_orig_idx)
            if fixed_end_node_manager_idx != -1 and depot_manager_idx != -1:
                print_debug(f"  Applying DROPOFF constraint: Node {fixed_end_node_orig_idx} -> Depot ({depot_original_idx}) (Manager indices: {fixed_end_node_manager_idx} -> {depot_manager_idx}) for vehicle {vehicle_id_for_reopt}")
                # Force the fixed_end_node to be followed by the depot
                routing.solver().Add(routing.NextVar(fixed_end_node_manager_idx) == depot_manager_idx)
                routing.solver().Add(routing.VehicleVar(fixed_end_node_manager_idx) == vehicle_id_for_reopt)
                routing.solver().Add(routing.VehicleVar(depot_manager_idx) == vehicle_id_for_reopt)

                if other_customers_original_indices:
                    for other_cust_orig_idx in other_customers_original_indices:
                        if not (0 <= other_cust_orig_idx < num_locations):
                            print_error(f"Invalid other_customer_node_index: {other_cust_orig_idx}, skipping constraint for it.")
                            continue
                        if other_cust_orig_idx == fixed_end_node_orig_idx: # Should not happen
                            continue

                        other_cust_manager_idx = manager.NodeToIndex(other_cust_orig_idx)
                        if other_cust_manager_idx != -1:
                            print_debug(f"    Constraint: Other customer {other_cust_orig_idx} (Manager: {other_cust_manager_idx}) cannot be followed by Depot for vehicle {vehicle_id_for_reopt}.")
                            routing.solver().Add(routing.NextVar(other_cust_manager_idx) != depot_manager_idx)
                            routing.solver().Add(routing.VehicleVar(other_cust_manager_idx) == vehicle_id_for_reopt)
                        else:
                            print_error(f"Could not get manager index for other_customer_node {other_cust_orig_idx}")
            else:
                print_error(f"Could not get manager index for fixed_end_node {fixed_end_node_orig_idx} or depot {depot_original_idx}")
    # --- END ADD CONSTRAINTS FOR FIXED NODES ---


    # --- Add Capacity constraint ---
    print_debug("  Registering demand callback and capacity dimension...")
    def demand_callback(from_index):
        from_node = manager.IndexToNode(from_index)
        if not (0 <= from_node < len(data_model["demands"])):
            print_error(f"Demand cb: Invalid node index. from_node={from_node}, N_demands={len(data_model['demands'])}")
            return 999999 # Large demand to make infeasible
        return data_model["demands"][from_node]
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index, 0, data_model["vehicle_capacities"], True, "Capacity"
    )
    print_debug("  Capacity dimension added.")

    # --- Add Max Route Duration constraint ---
    if data_model["max_route_duration"] is not None:
        print_debug("  Registering duration callback for Max Route Duration dimension...")
        def service_plus_travel_for_dimension_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            if not (0 <= from_node < num_locations and 0 <= to_node < num_locations):
                print_error(f"ServicePlusTravelDuration cb for dimension: Invalid node index. from_node={from_node}, to_node={to_node}, N={num_locations}")
                return data_model.get("large_penalty_int", 999999999) # Large duration
            service_time = data_model["service_times"][from_node]
            travel_time = data_model["duration_matrix"][from_node][to_node]
            return service_time + travel_time

        duration_dimension_callback_index = routing.RegisterTransitCallback(service_plus_travel_for_dimension_callback)
        routing.AddDimension(
            duration_dimension_callback_index,
            0, # No slack
            data_model["max_route_duration"],
            False, # Start cumul at zero for depot (True would allow pre-depot travel time)
            "MaxDuration",
        )
        print_debug(f"  Max Route Duration dimension added with max: {data_model['max_route_duration']} (based on actual service+travel time).")
    else:
        print_debug("  Max_route_duration not set, skipping duration constraint.")

    # --- Allow dropping nodes ---
    # For re-optimization runs, Node.js should send allow_dropping_visits = False.
    # If it's True, and fixed nodes are set, those fixed nodes should ideally have a penalty of 0.
    # However, the NextVar constraints for fixed nodes make them effectively mandatory.
    if data_model["allow_dropping_visits"]:
        print_debug(f"  Allowing nodes to be dropped with penalty: {data_model['drop_visit_penalty']}")
        penalty_value = data_model['drop_visit_penalty']
        for node_idx_in_model in range(num_locations): # Iterate through all model nodes (original indices)
            if node_idx_in_model == depot_original_idx:
                continue

            # Do not allow dropping for fixed start/end nodes if they are set
            is_fixed_node = (fixed_start_node_orig_idx is not None and node_idx_in_model == fixed_start_node_orig_idx) or \
                            (fixed_end_node_orig_idx is not None and node_idx_in_model == fixed_end_node_orig_idx)

            # Also, for dropoff re-optimization, other customers in the route should not be dropped.
            is_other_fixed_customer = False
            if fixed_end_node_orig_idx is not None and data_model.get("other_customer_node_indices_in_matrix"):
                if node_idx_in_model in data_model["other_customer_node_indices_in_matrix"]:
                    is_other_fixed_customer = True

            current_penalty = 0 if (is_fixed_node or is_other_fixed_customer) else penalty_value
            if is_fixed_node or is_other_fixed_customer:
                 print_debug(f"    Node {node_idx_in_model} is part of a fixed re-optimization, making it mandatory (penalty 0 if dropping allowed).")


            routing_idx = manager.NodeToIndex(node_idx_in_model)
            if routing_idx != -1:
                 routing.AddDisjunction([routing_idx], current_penalty)
    else:
        print_debug("  Dropping visits not allowed. All non-depot nodes are mandatory unless capacity/duration makes solution infeasible.")


    # --- Search Parameters ---
    print_debug("  Setting search parameters...")
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC # Often good, can try others like PATH_CHEAPEST_ARC or SWEEP
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC # Can try others like SIMULATED_ANNEALING or TABU_SEARCH
    )
    print_debug(f"  First solution strategy: {search_parameters.first_solution_strategy}, Local search: {search_parameters.local_search_metaheuristic}")


    time_limit_seconds = 10 # Default
    if num_locations <= 5:  time_limit_seconds = 5
    elif num_locations <= 10: time_limit_seconds = 15
    elif num_locations <= 15: time_limit_seconds = 30
    elif num_locations <= 20: time_limit_seconds = 45
    elif num_locations <= 25: time_limit_seconds = 60
    elif num_locations <= 30: time_limit_seconds = 90
    else: time_limit_seconds = 120

    # If it's a re-optimization run (likely small N), keep time limit short
    if fixed_start_node_orig_idx is not None or fixed_end_node_orig_idx is not None:
        time_limit_seconds = min(time_limit_seconds, 10) # Shorter time for re-opt
        print_debug(f"  Re-optimization run detected, reducing time limit if it was higher.")

    print_debug(f"  Setting dynamic solver time limit to: {time_limit_seconds} seconds for {num_locations} locations.")
    search_parameters.time_limit.FromSeconds(time_limit_seconds)
    # search_parameters.log_search = True # Enable for detailed solver logs

    # --- Solve ---
    print_debug("  Starting solver...")
    solution = routing.SolveWithParameters(search_parameters)

    # --- Process Solution ---
    status_value = routing.status()
    status_name = f"STATUS_INT_{status_value}"
    try:
        status_name = routing_enums_pb2.RoutingSearchStatus.DESCRIPTOR.values_by_number[status_value].name
    except (AttributeError, KeyError):
        print_error(f"Could not get string name for status value {status_value} using DESCRIPTOR. Trying direct Name access...")
        try:
            status_name = routing_enums_pb2.RoutingSearchStatus.Name(status_value)
        except (AttributeError, KeyError, ValueError):
             print_error(f"Direct Name access for status value {status_value} also failed. Using integer value as name.")
    print_debug(f"  Solver finished. Status: {status_value} ({status_name})")

    output_routes = []
    dropped_node_indices = [] # Store original indices

    if solution:
        print_debug(f"  OR-Tools solution object exists. Status was: {status_value} ({status_name})")
        total_distance = 0
        total_load = 0
        for vehicle_id in range(data_model["num_vehicles"]):
            index = routing.Start(vehicle_id)
            route_for_vehicle_original_indices = []
            route_distance = 0
            route_load = 0
            while not routing.IsEnd(index):
                node_original_idx = manager.IndexToNode(index)
                # Only add customer nodes to the route list, not the depot itself if it's not the start/end placeholder
                # The first node_original_idx will be the depot.
                # The last node before IsEnd() will also be the depot.
                # We want the sequence of *customer* visits.
                if node_original_idx != depot_original_idx:
                    route_for_vehicle_original_indices.append(node_original_idx)

                previous_index = index
                index = solution.Value(routing.NextVar(index)) # Get manager index of next stop

                # Calculate route distance using the solver's cost, not just raw matrix
                # This includes penalties if the cost callback has them.
                # cost_eval_idx = routing.GetArcCostForVehicle(previous_index, index, vehicle_id) # This is not directly available
                # Instead, use the registered callback
                from_node_calc = manager.IndexToNode(previous_index)
                to_node_calc = manager.IndexToNode(index)
                if from_node_calc < num_locations and to_node_calc < num_locations : # Check bounds
                    arc_cost = distance_with_direction_penalty_callback(previous_index, index) # This uses the penalized cost
                    route_distance += arc_cost

                # Calculate load (optional, for debugging)
                if node_original_idx != depot_original_idx: # Don't add depot demand
                    route_load += data_model["demands"][node_original_idx]


            if route_for_vehicle_original_indices: # Only add non-empty routes (excluding depot start/end)
                output_routes.append(route_for_vehicle_original_indices)
                print_debug(f"    Vehicle {vehicle_id} route (original node indices): {route_for_vehicle_original_indices}, Distance (penalized): {route_distance}, Load: {route_load}")
                total_distance += route_distance
                total_load += route_load
            elif routing.IsEnd(routing.Start(vehicle_id)) and not routing.IsEnd(index): # Vehicle used but no customers
                 print_debug(f"    Vehicle {vehicle_id} was used but served no customers (empty tour).")


        print_debug(f"  Total penalized distance for all routes: {total_distance}")
        print_debug(f"  Total load for all routes: {total_load}")


        if data_model["allow_dropping_visits"]:
            for node_original_idx in range(num_locations):
                if node_original_idx == depot_original_idx:
                    continue
                # Check if this node (original index) was dropped
                # A node is dropped if it's part of a disjunction and its ActiveVar is false.
                manager_routing_idx = manager.NodeToIndex(node_original_idx)
                if manager_routing_idx != -1: # Check if node is valid in routing model
                    # Check if the node is part of any disjunction
                    # This check is a bit indirect. A simpler way: if it's not in any route.
                    # However, the most robust way is to check its disjunction status if AddDisjunction was used.
                    # For AddDisjunction([node], penalty), if penalty > 0 and node is dropped, ActiveVar(node) is 0.
                    # If penalty is 0 (mandatory), ActiveVar(node) should be 1.

                    # Check if the node is in any of the output_routes
                    is_in_a_route = False
                    for r_nodes in output_routes:
                        if node_original_idx in r_nodes:
                            is_in_a_route = True
                            break

                    if not is_in_a_route:
                        # If allow_dropping_visits was true, and it's not in a route, it was likely dropped.
                        # The AddDisjunction call handles this. We can verify by checking ActiveVar if needed,
                        # but simply not being in a route is a strong indicator when dropping is allowed.
                        # For fixed nodes (penalty 0 in disjunction), they should be in a route.
                        # If they are not, it means the problem was infeasible with them.
                        is_fixed_node = (fixed_start_node_orig_idx is not None and node_original_idx == fixed_start_node_orig_idx) or \
                                        (fixed_end_node_orig_idx is not None and node_original_idx == fixed_end_node_orig_idx)
                        is_other_fixed_customer = False
                        if fixed_end_node_orig_idx is not None and data_model.get("other_customer_node_indices_in_matrix"):
                            if node_original_idx in data_model["other_customer_node_indices_in_matrix"]:
                                is_other_fixed_customer = True

                        if is_fixed_node or is_other_fixed_customer:
                            print_error(f"    Mandatory node {node_original_idx} (fixed/other_fixed) was NOT found in any route. This indicates a potential infeasibility with the fixed constraints.")
                            # Still add to dropped_node_indices as it wasn't served.
                            dropped_node_indices.append(node_original_idx)
                        else:
                            # Regular node that could be dropped
                            dropped_node_indices.append(node_original_idx)
                            print_debug(f"    Node {node_original_idx} (original index) was not in any route and presumed dropped.")
                else:
                    print_error(f"Could not get manager index for node {node_original_idx} during dropped node check.")


            if dropped_node_indices:
                print_debug(f"    Dropped node original indices (depot is {depot_original_idx}): {dropped_node_indices}")

        if not output_routes and num_locations > 1 and data_model["num_vehicles"] > 0 :
            # Check if there were customers (num_locations > 1 if depot is one location)
            # And if no nodes were dropped (when dropping is not allowed or all were mandatory)
            is_reopt_run = fixed_start_node_orig_idx is not None or fixed_end_node_orig_idx is not None
            if not data_model["allow_dropping_visits"] and not dropped_node_indices and not is_reopt_run:
                 print_error(f"Solver status ({status_name}) with solution object, but no routes extracted and no nodes dropped (and dropping not allowed). This might indicate an issue or all customers unserviceable.")
            elif is_reopt_run and not dropped_node_indices: # Re-opt should not drop
                 print_error(f"Re-optimization run resulted in no routes and no dropped nodes. This is unexpected. Status: {status_name}")


    else: # No solution object
        print_error(f"No solution object returned by OR-Tools. Status: {status_value} ({status_name})")
        # If no solution, all non-depot nodes are effectively dropped if dropping is allowed,
        # or it indicates infeasibility if dropping is not allowed.
        if data_model["allow_dropping_visits"]:
            for i in range(num_locations):
                if i != depot_original_idx:
                    dropped_node_indices.append(i)
            print_debug(f"    All non-depot nodes considered dropped due to no solution object (and dropping allowed). Dropped: {dropped_node_indices}")
        elif num_locations > 1 : # If there were customers to serve
             print_error(f"    No solution and dropping not allowed. Problem likely infeasible. All non-depot nodes considered unserved.")
             for i in range(num_locations):
                if i != depot_original_idx:
                    dropped_node_indices.append(i) # Report them as "dropped" or "unserved"


    return {"routes": output_routes, "dropped_node_indices": dropped_node_indices }


# --- Main Execution ---
if __name__ == "__main__":
    try:
        raw_input_data = sys.stdin.read()
        print_debug(f"[SCRIPT_START] Received raw data length: {len(raw_input_data)}")
        if not raw_input_data:
            print_error("No input data received from stdin.")
            print(json.dumps({"error": "No input data", "routes": [], "dropped_node_indices": []}), file=sys.stderr)
            sys.exit(1)

        input_json = json.loads(raw_input_data)
        print_debug("  Successfully parsed JSON input.")

        model_data = create_data_model(input_json)
        result = solve_cvrptw(model_data)

        print_debug(f"  Sending result back to Node: {json.dumps(result)}")
        print(json.dumps(result)) # Print result JSON to stdout
    except json.JSONDecodeError as je:
        print_error(f"JSONDecodeError: {str(je)}. Raw data snippet: {raw_input_data[:500]}...")
        # Output valid JSON to stderr for Node.js to parse as an error object
        error_response = {"error": f"JSONDecodeError: {str(je)}", "details": repr(je), "routes": [], "dropped_node_indices": []}
        print(json.dumps(error_response), file=sys.stderr)
        # Also print to stdout for the calling process to capture if it only reads stdout for the final result
        print(json.dumps(error_response))
        sys.exit(1)
    except ValueError as ve: # Catch validation errors from create_data_model
        print_error(f"ValueError: {str(ve)}. Details: {repr(ve)}")
        traceback.print_exc(file=sys.stderr)
        error_response = {"error": str(ve), "details": repr(ve), "routes": [], "dropped_node_indices": []}
        print(json.dumps(error_response), file=sys.stderr)
        print(json.dumps(error_response))
        sys.exit(1)
    except Exception as e:
        print_error(f"Unhandled Exception: {str(e)}. Details: {repr(e)}")
        traceback.print_exc(file=sys.stderr)
        error_response = {"error": str(e), "details": repr(e), "routes": [], "dropped_node_indices": []}
        print(json.dumps(error_response), file=sys.stderr)
        print(json.dumps(error_response))
        sys.exit(1)
