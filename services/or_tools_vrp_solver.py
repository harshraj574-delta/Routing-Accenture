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

# Optional: Haversine function (not used by the cost callback if matrix is complete)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
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
    large_penalty_int = 999999999

    try:
        data["distance_matrix"] = [
            [int(round(cost)) if cost is not None else large_penalty_int for cost in row]
            for row in input_data["distance_matrix"]
        ]
    except (TypeError, ValueError) as e:
        print_error(f"Error processing distance_matrix: {e}. Input was: {input_data.get('distance_matrix')}")
        raise

    # Duration matrix is loaded but NOT used by the primary cost callback in this version
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
    data["depot"] = int(input_data.get("depot_index", 0))

    max_dur = input_data.get("max_route_duration", None)
    data["max_route_duration"] = int(round(max_dur)) if max_dur is not None else None

    service_times_input = input_data.get("service_times", [0] * len(data["demands"]))
    data["service_times"] = [int(round(st)) if st is not None else 0 for st in service_times_input]

    data["allow_dropping_visits"] = input_data.get("allow_dropping_visits", False)
    data["drop_visit_penalty"] = int(input_data.get("drop_visit_penalty", 5000000))

    data["facility_coords"] = input_data.get("facility_coords")
    data["trip_type"] = input_data.get("trip_type", "PICKUP").upper()
    data["direction_penalty_weight"] = float(input_data.get("direction_penalty_weight", 1.0))

    data["fixed_start_node_index_in_matrix"] = input_data.get("fixed_start_node_index_in_matrix", None)
    data["fixed_end_node_index_in_matrix"] = input_data.get("fixed_end_node_index_in_matrix", None)
    data["other_customer_node_indices_in_matrix"] = input_data.get("other_customer_node_indices_in_matrix", None)

    num_locations = len(data['distance_matrix'])
    print_debug(f"  Num locations: {num_locations}, Num vehicles: {data['num_vehicles']}, Depot: {data['depot']}")
    # ... (rest of the print_debugs and validations from your old script can be kept) ...
    if num_locations > 0:
        if not all(len(row) == num_locations for row in data['distance_matrix']):
            raise ValueError("Distance matrix is not square.")
        if len(data['demands']) != num_locations:
            raise ValueError("Demands length mismatch.")
        if len(data['service_times']) != num_locations:
            raise ValueError("Service times length mismatch.")
    if data["depot"] != 0:
         print_error(f"Depot index is assumed to be 0 for some logic, but received {data['depot']}.")
    if data["num_vehicles"] > 0 and len(data["vehicle_capacities"]) != data["num_vehicles"]:
        raise ValueError("Vehicle capacities length mismatch.")
    # ... (fixed node validations) ...

    return data

# --- Solver ---
def solve_cvrptw(data_model):
    print_debug("Initializing OR-Tools Routing Manager and Model...")
    num_locations = len(data_model["distance_matrix"])
    depot_original_idx = data_model["depot"]

    if num_locations == 0:
        return {"routes": [], "dropped_node_indices": [], "error": "Empty distance matrix"}
    if data_model["num_vehicles"] == 0 and num_locations > 1:
        return {"routes": [], "dropped_node_indices": [], "error": "0 vehicles with locations to visit"}

    manager = pywrapcp.RoutingIndexManager(
        num_locations, data_model["num_vehicles"], depot_original_idx
    )
    routing = pywrapcp.RoutingModel(manager)
    print_debug("  Manager and Model created.")
    depot_manager_idx = manager.NodeToIndex(depot_original_idx)

    # --- *** ADOPTED COST CALLBACK (from your "old" script) *** ---
    # This callback uses data_model["distance_matrix"] as the primary cost.
    def distance_with_direction_penalty_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)

        if not (0 <= from_node < num_locations and 0 <= to_node < num_locations):
            print_error(f"Cost cb: Invalid node index. from_node={from_node}, to_node={to_node}, N={num_locations}")
            return data_model.get("large_penalty_int", 999999999)

        travel_distance = data_model["distance_matrix"][from_node][to_node]
        penalty = 0
        weight = data_model["direction_penalty_weight"]

        if from_node != depot_original_idx and to_node != depot_original_idx and weight > 0:
            try:
                dist_from_to_depot = data_model["distance_matrix"][from_node][depot_original_idx]
                dist_to_to_depot = data_model["distance_matrix"][to_node][depot_original_idx]

                if data_model["trip_type"] == "PICKUP":
                    distance_increase_from_depot = dist_to_to_depot - dist_from_to_depot
                    if distance_increase_from_depot > 0:
                        penalty = weight * distance_increase_from_depot
                elif data_model["trip_type"] == "DROPOFF":
                    distance_decrease_to_depot = dist_from_to_depot - dist_to_to_depot
                    if distance_decrease_to_depot > 0:
                        penalty = (weight * distance_decrease_to_depot)
            except IndexError:
                 print_error(f"Cost cb: IndexError for depot distances. from={from_node}, to={to_node}, depot={depot_original_idx}")
            except Exception as e:
                 print_error(f"Cost cb: Error calculating penalty: {e}")
        final_cost = travel_distance + penalty
        return int(round(final_cost))

    modified_cost_callback_index = routing.RegisterTransitCallback(
        distance_with_direction_penalty_callback
    )
    routing.SetArcCostEvaluatorOfAllVehicles(modified_cost_callback_index)
    print_debug(f"  Arc cost set to DISTANCE + DIRECTION PENALTY (Weight: {data_model['direction_penalty_weight']}).")

    # --- Constraints (Capacity, Duration, Fixed Nodes, Dropping) ---
    # These are largely the same as your "old" script.

    # Capacity
    def demand_callback(from_index):
        from_node = manager.IndexToNode(from_index)
        return data_model["demands"][from_node]
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index, 0, data_model["vehicle_capacities"], True, "Capacity"
    )

    # Max Route Duration (uses duration_matrix and service_times)
    if data_model["max_route_duration"] is not None:
        def service_plus_travel_for_duration_dim_callback(from_index, to_index): # Renamed for clarity
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            if not (0 <= from_node < num_locations and 0 <= to_node < num_locations):
                return data_model.get("large_penalty_int", 999999999)
            # This dimension uses DURATION matrix, even if cost callback uses DISTANCE
            service_time = data_model["service_times"][from_node]
            travel_time = data_model["duration_matrix"][from_node][to_node]
            return service_time + travel_time
        duration_dim_callback_index = routing.RegisterTransitCallback(service_plus_travel_for_duration_dim_callback)
        routing.AddDimension(
            duration_dim_callback_index, 0, data_model["max_route_duration"],
            False, "MaxDuration"
        )
        print_debug(f"  Max Route Duration dimension added (uses DURATION matrix).")

    # Fixed Nodes (logic from your old script)
    vehicle_id_for_reopt = 0
    fixed_start_node_orig_idx = data_model.get("fixed_start_node_index_in_matrix")
    fixed_end_node_orig_idx = data_model.get("fixed_end_node_index_in_matrix")
    if fixed_start_node_orig_idx is not None:
        if 0 <= fixed_start_node_orig_idx < num_locations:
            fixed_start_node_manager_idx = manager.NodeToIndex(fixed_start_node_orig_idx)
            if fixed_start_node_manager_idx != -1 and depot_manager_idx != -1:
                routing.solver().Add(routing.NextVar(depot_manager_idx) == fixed_start_node_manager_idx)
                routing.solver().Add(routing.VehicleVar(fixed_start_node_manager_idx) == vehicle_id_for_reopt)
                routing.solver().Add(routing.VehicleVar(depot_manager_idx) == vehicle_id_for_reopt)
                print_debug(f"  Applied fixed start node constraint: Depot -> {fixed_start_node_orig_idx}")
    elif fixed_end_node_orig_idx is not None:
        if 0 <= fixed_end_node_orig_idx < num_locations:
            fixed_end_node_manager_idx = manager.NodeToIndex(fixed_end_node_orig_idx)
            other_customers_original_indices = data_model.get("other_customer_node_indices_in_matrix", [])
            if fixed_end_node_manager_idx != -1 and depot_manager_idx != -1:
                routing.solver().Add(routing.NextVar(fixed_end_node_manager_idx) == depot_manager_idx)
                routing.solver().Add(routing.VehicleVar(fixed_end_node_manager_idx) == vehicle_id_for_reopt)
                routing.solver().Add(routing.VehicleVar(depot_manager_idx) == vehicle_id_for_reopt)
                print_debug(f"  Applied fixed end node constraint: {fixed_end_node_orig_idx} -> Depot")
                if other_customers_original_indices:
                    for other_cust_orig_idx in other_customers_original_indices:
                        if 0 <= other_cust_orig_idx < num_locations and other_cust_orig_idx != fixed_end_node_orig_idx:
                            other_cust_manager_idx = manager.NodeToIndex(other_cust_orig_idx)
                            if other_cust_manager_idx != -1:
                                routing.solver().Add(routing.NextVar(other_cust_manager_idx) != depot_manager_idx)
                                routing.solver().Add(routing.VehicleVar(other_cust_manager_idx) == vehicle_id_for_reopt)


    # Allow Dropping Nodes (logic from your old script)
    if data_model["allow_dropping_visits"]:
        penalty_value = data_model['drop_visit_penalty']
        for node_idx_in_model in range(num_locations):
            if node_idx_in_model == depot_original_idx: continue
            is_fixed = (fixed_start_node_orig_idx == node_idx_in_model) or \
                       (fixed_end_node_orig_idx == node_idx_in_model) or \
                       (data_model.get("other_customer_node_indices_in_matrix") and \
                        node_idx_in_model in data_model["other_customer_node_indices_in_matrix"])
            current_penalty = 0 if is_fixed else penalty_value
            if is_fixed: print_debug(f"    Node {node_idx_in_model} is fixed, drop penalty 0.")
            routing.AddDisjunction([manager.NodeToIndex(node_idx_in_model)], current_penalty)
        print_debug(f"  Allowing nodes to be dropped with penalty: {penalty_value}")


    # --- Search Parameters (Typical for Node.js integration) ---
    print_debug("  Setting search parameters...")
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC # Common default
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC # Good local search
    )
    # search_parameters.solution_limit = 1 # Often set if only one solution is needed quickly
    search_parameters.log_search = False # Typically false for production

    time_limit_seconds = 5
    if num_locations <= 5:  time_limit_seconds = 3
    elif num_locations <= 10: time_limit_seconds = 5
    elif num_locations <= 15: time_limit_seconds = 8
    elif num_locations <= 20: time_limit_seconds = 10
    else: time_limit_seconds = 15 # More conservative than your old script's default

    if fixed_start_node_orig_idx is not None or fixed_end_node_orig_idx is not None:
        time_limit_seconds = min(time_limit_seconds, 5)
    print_debug(f"  Solver time limit: {time_limit_seconds}s for {num_locations} locations.")
    search_parameters.time_limit.FromSeconds(time_limit_seconds)

    # --- Solve ---
    print_debug("  Starting solver...")
    solution = routing.SolveWithParameters(search_parameters)

    # --- Process Solution (MODIFIED for Node.js expected output) ---
    status_value = routing.status()
    status_name = f"STATUS_INT_{status_value}"
    try:
        status_name = routing_enums_pb2.RoutingSearchStatus.DESCRIPTOR.values_by_number[status_value].name
    except: # Broad except to catch any error in getting name
        try: status_name = routing_enums_pb2.RoutingSearchStatus.Name(status_value)
        except: print_error(f"Could not get string name for status value {status_value}.")
    print_debug(f"  Solver finished. Status: {status_value} ({status_name})")

    output_routes_list_of_objects = [] # Changed name for clarity
    dropped_node_indices = []

    if solution:
        print_debug(f"  Solution object exists. Status: {status_name}")
        for vehicle_id in range(data_model["num_vehicles"]):
            index = routing.Start(vehicle_id)
            route_nodes_for_vehicle = [] # Customer nodes for this vehicle
            # route_penalized_distance = 0 # If you want to calculate and log this per route

            while not routing.IsEnd(index):
                node_original_idx = manager.IndexToNode(index)
                # previous_manager_idx = index # For calculating arc cost if needed
                index = solution.Value(routing.NextVar(index))

                if node_original_idx != depot_original_idx: # Add customer nodes
                    route_nodes_for_vehicle.append(node_original_idx)
                # arc_cost = distance_with_direction_penalty_callback(previous_manager_idx, index)
                # route_penalized_distance += arc_cost

            if route_nodes_for_vehicle: # If the route served any customers
                output_routes_list_of_objects.append({
                    "vehicle_index": vehicle_id,
                    "node_indices": route_nodes_for_vehicle
                })
                print_debug(f"    Vehicle {vehicle_id} route (original cust indices): {route_nodes_for_vehicle}")
            elif not routing.IsEnd(routing.Start(vehicle_id)) and routing.IsEnd(index) and routing.Start(vehicle_id) != index :
                 print_debug(f"    Vehicle {vehicle_id} was used but served no customers (depot-to-depot tour).")


        if data_model["allow_dropping_visits"]:
            for node_idx in range(num_locations):
                if node_idx == depot_original_idx: continue
                manager_idx = manager.NodeToIndex(node_idx)
                if manager_idx != -1 and routing.IsStart(solution.Value(routing.NextVar(manager_idx))) and routing.IsEnd(solution.Value(routing.NextVar(manager_idx))):
                     # A more direct way to check if a node is dropped when disjunctions are used:
                     # if not solution.Value(routing.ActiveVar(manager.NodeToIndex(node_idx))):
                     # However, checking if it's in any route is simpler if ActiveVar isn't directly queried.
                    is_in_a_route = False
                    for r_obj in output_routes_list_of_objects:
                        if node_idx in r_obj["node_indices"]:
                            is_in_a_route = True
                            break
                    if not is_in_a_route:
                        # Check if it was a fixed node that shouldn't have been dropped
                        is_fixed = (fixed_start_node_orig_idx == node_idx) or \
                                   (fixed_end_node_orig_idx == node_idx) or \
                                   (data_model.get("other_customer_node_indices_in_matrix") and \
                                    node_idx in data_model["other_customer_node_indices_in_matrix"])
                        if is_fixed:
                            print_error(f"    Mandatory node {node_idx} was NOT found in any route (dropped). Infeasibility likely.")
                        dropped_node_indices.append(node_idx)
            if dropped_node_indices: print_debug(f"    Dropped node original indices: {dropped_node_indices}")

    else: # No solution object
        print_error(f"No solution object. Status: {status_name}")
        if data_model["allow_dropping_visits"]:
            for i in range(num_locations):
                if i != depot_original_idx: dropped_node_indices.append(i)
            print_debug(f"    All non-depot nodes considered dropped (allow_dropping_visits=True).")
        elif num_locations > 1:
            print_error(f"    No solution and dropping not allowed. Problem likely infeasible.")
            for i in range(num_locations): # Report all as unserved/dropped
                if i != depot_original_idx: dropped_node_indices.append(i)


    return {"routes": output_routes_list_of_objects, "dropped_node_indices": dropped_node_indices}


# --- Main Execution ---
if __name__ == "__main__":
    try:
        raw_input_data = sys.stdin.read()
        print_debug(f"[SCRIPT_START] Received raw data length: {len(raw_input_data)}")
        if not raw_input_data:
            # ... (error handling as before) ...
            print(json.dumps({"error": "No input data", "routes": [], "dropped_node_indices": []}), file=sys.stderr)
            sys.exit(1)

        input_json = json.loads(raw_input_data)
        print_debug("  Successfully parsed JSON input.")

        model_data = create_data_model(input_json)
        result = solve_cvrptw(model_data)

        print_debug(f"  Sending result back to Node: {json.dumps(result)}")
        print(json.dumps(result))
    except json.JSONDecodeError as je:
        # ... (error handling as before) ...
        error_response = {"error": f"JSONDecodeError: {str(je)}", "details": repr(je), "routes": [], "dropped_node_indices": []}
        print(json.dumps(error_response), file=sys.stderr)
        print(json.dumps(error_response))
        sys.exit(1)
    except ValueError as ve:
        # ... (error handling as before) ...
        error_response = {"error": str(ve), "details": repr(ve), "routes": [], "dropped_node_indices": []}
        print(json.dumps(error_response), file=sys.stderr)
        print(json.dumps(error_response))
        sys.exit(1)
    except Exception as e:
        # ... (error handling as before) ...
        error_response = {"error": str(e), "details": repr(e), "routes": [], "dropped_node_indices": []}
        print(json.dumps(error_response), file=sys.stderr)
        print(json.dumps(error_response))
        sys.exit(1)

