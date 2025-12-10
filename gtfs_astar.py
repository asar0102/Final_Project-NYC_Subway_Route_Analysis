import sqlite3
import pandas as pd
import networkx as nx
import math
import os

# Configuration
db_path = 'subway_network.db'
SUBWAY_SPEED_MPS = 10  # Estimate: 10 meters/sec (approx 36 km/h or 22 mph)
R = 6371000  # Radius of Earth in meters

# Heuristic
def haversine(lat1, lon1, lat2, lon2):
    """Calculates the great-circle distance between two points (in meters)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi / 2)**2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(dlambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def build_network_and_predict():
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_path}' not found. Run Part 1 script first.")
        return

    print("--- Part 2: Network Analysis & A* Prediction ---")
    conn = sqlite3.connect(db_path)

    # 1. Load Nodes (Stops with Coordinates)
    print("Loading stops...")
    stops_query = "SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops"
    stops_df = pd.read_sql(stops_query, conn)
    
    # Create lookup dicts for names and coordinates
    stop_name_map = stops_df.set_index('stop_id')['stop_name'].to_dict()
    stop_coords = stops_df.set_index('stop_id')[['stop_lat', 'stop_lon']].to_dict('index')

    # 2. Load Edges (Trip Segments & Transfers)
    print("Loading network edges...")
    
    # Trip Edges: Get minimum scheduled time between connected stops
    trip_query = """
    SELECT from_stop_id, to_stop_id, MIN(duration_sec) as weight, route_id
    FROM trip_segments 
    GROUP BY from_stop_id, to_stop_id
    """
    trip_edges_df = pd.read_sql(trip_query, conn)

    # Transfer Edges: Use minimum transfer time (default to 3 mins/180s if missing)
    transfer_query = """
    SELECT from_stop_id, to_stop_id, 
        CASE WHEN min_transfer_time > 0 THEN min_transfer_time ELSE 180 END as weight
    FROM transfers 
    WHERE from_stop_id != to_stop_id
    """
    transfer_edges_df = pd.read_sql(transfer_query, conn)
    conn.close()

    # 3. Build the Graph
    G = nx.DiGraph()
    
    # Add Nodes
    G.add_nodes_from(stops_df['stop_id'])
    nx.set_node_attributes(G, stop_name_map, 'name')
    nx.set_node_attributes(G, stop_coords, 'coords')

    # Add Edges
    for _, row in trip_edges_df.iterrows():
        G.add_edge(row['from_stop_id'], row['to_stop_id'], 
                   weight=row['weight'], type='travel', route=row['route_id'])
                   
    for _, row in transfer_edges_df.iterrows():
        if G.has_node(row['from_stop_id']) and G.has_node(row['to_stop_id']):
            G.add_edge(row['from_stop_id'], row['to_stop_id'], 
                       weight=row['weight'], type='transfer')

    print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")

    # 4. Define A* Heuristic Function
    def heuristic(u, v):
        # Estimate travel time between stop u and stop v based on distance.
        try:
            # Get coordinates from the graph node attributes
            u_coords = G.nodes[u]['coords']
            v_coords = G.nodes[v]['coords']
            
            dist = haversine(u_coords['stop_lat'], u_coords['stop_lon'],
                             v_coords['stop_lat'], v_coords['stop_lon'])
            
            # Return estimated time in seconds
            return dist / SUBWAY_SPEED_MPS
        except KeyError:
            return 0

    # 5. Execute Prediction
    # Example: Van Cortlandt Park (101S) -> South Ferry (142S)
    start_node = '101S'
    end_node = '142S' 
    
    print(f"\nCalculating shortest path from {stop_name_map.get(start_node, start_node)} to {stop_name_map.get(end_node, end_node)}...")

    if G.has_node(start_node) and G.has_node(end_node):
        try:
            # Run A* Algorithm
            path = nx.astar_path(G, start_node, end_node, heuristic=heuristic, weight='weight')
            total_seconds = nx.astar_path_length(G, start_node, end_node, heuristic=heuristic, weight='weight')
            
            print("\n--- Route Prediction Successful (A*) ---")
            print(f"Origin:      {stop_name_map[start_node]} ({start_node})")
            print(f"Destination: {stop_name_map[end_node]} ({end_node})")
            print(f"Total Time:  {total_seconds / 60:.1f} minutes")
            print(f"Stops:       {len(path) - 1}")
            
            # Optional: Print the first few steps of the path
            print("\nRoute Segment (First 5 stops):")
            for i in range(min(5, len(path)-1)):
                u, v = path[i], path[i+1]
                edge = G[u][v]
                mode = f"Take {edge.get('route')} line" if edge.get('type') == 'travel' else "Transfer/Walk"
                print(f"  {i+1}. {stop_name_map.get(u)} -> {stop_name_map.get(v)} ({mode}, {edge['weight']}s)")
                
        except nx.NetworkXNoPath:
            print(f"No path found between {start_node} and {end_node}.")
    else:
        print("Error: Start or End node not found in graph.")

if __name__ == "__main__":
    build_network_and_predict()