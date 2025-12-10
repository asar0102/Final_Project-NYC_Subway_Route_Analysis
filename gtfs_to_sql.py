import pandas as pd
import zipfile
import sqlite3
import os

# Configuration
zip_file_path = 'gtfs_subway.zip'
db_path = 'subway_network.db'

def gtfs_time_to_seconds(time_str):
    """Converts GTFS 'HH:MM:SS' string to seconds from midnight.
    Handles times > 24:00:00 (e.g., 25:30:00)."""
    if pd.isna(time_str):
        return None
    try:
        parts = time_str.split(':')
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except:
        return None

def build_database():
    # 1. Connect to SQLite (creates file if does not exists)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Building database at: {db_path}")

    with zipfile.ZipFile(zip_file_path, 'r') as z:
        # --- A. Load Static Tables ---
        # We load these directly as they don't need complex transformation
        static_files = ['stops.txt', 'routes.txt', 'trips.txt', 'calendar.txt', 'transfers.txt']
        
        for filename in static_files:
            if filename in z.namelist():
                print(f"Loading {filename} into SQL...")
                df = pd.read_csv(z.open(filename))
                # Remove table if exists
                table_name = filename.replace('.txt', '')
                df.to_sql(table_name, conn, if_exists='replace', index=False)

        # --- B. Load & Transform stop_times.txt ---
        # This needs special handling for time calculation
        if 'stop_times.txt' in z.namelist():
            print("Loading and transforming stop_times.txt (this may take a moment)...")
            df_st = pd.read_csv(z.open('stop_times.txt'))
            
            # Convert HH:MM:SS to Seconds for math
            df_st['arrival_time_sec'] = df_st['arrival_time'].apply(gtfs_time_to_seconds)
            df_st['departure_time_sec'] = df_st['departure_time'].apply(gtfs_time_to_seconds)
            
            # Indexing columns to speed up the SQL window functions later
            df_st.to_sql('stop_times', conn, if_exists='replace', index=False)
            
            # Create indices for performance
            print("Creating indices...")
            cursor.execute("CREATE INDEX idx_trip_seq ON stop_times (trip_id, stop_sequence)")
            cursor.execute("CREATE INDEX idx_stop_id ON stop_times (stop_id)")

    # --- C. Feature Preparation: The Network Graph ---
    # We use SQL Window Functions to link current stop -> next stop
    print("Executing SQL to build Network Graph Edges...")
    
    create_graph_query = """
    CREATE TABLE trip_segments AS
    WITH OrderedStops AS (
        SELECT 
            trip_id,
            stop_id as from_stop_id,
            departure_time_sec as start_time_sec,
            stop_sequence,
            -- Look ahead to find the next stop in the sequence
            LEAD(stop_id) OVER (PARTITION BY trip_id ORDER BY stop_sequence) as to_stop_id,
            LEAD(arrival_time_sec) OVER (PARTITION BY trip_id ORDER BY stop_sequence) as end_time_sec
        FROM stop_times
    )
    SELECT 
        os.trip_id,
        os.from_stop_id,
        os.to_stop_id,
        os.start_time_sec,
        os.end_time_sec,
        -- TARGET VARIABLE: Calculate scheduled duration in seconds
        (os.end_time_sec - os.start_time_sec) as duration_sec,
        t.route_id,
        t.service_id,
        t.direction_id
    FROM OrderedStops os
    JOIN trips t ON os.trip_id = t.trip_id
    WHERE os.to_stop_id IS NOT NULL; -- Filter out the last stop of every trip
    """
    
    cursor.execute("DROP TABLE IF EXISTS trip_segments")
    cursor.execute(create_graph_query)
    
    conn.commit()
    print("Database build complete.")
    
    # --- D. Verification ---
    print("\n--- Sample Data: Network Graph Edges (First 5 Rows) ---")
    sample = pd.read_sql("SELECT * FROM trip_segments LIMIT 5", conn)
    print(sample)
    
    conn.close()

if __name__ == "__main__":
    build_database()