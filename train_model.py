import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import joblib
import os

# Configuration
db_path = 'subway_network.db'

def train_travel_time_model():
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_path}' not found.")
        return

    print("--- Part 3: Machine Learning Model Training ---")
    conn = sqlite3.connect(db_path)

    # 1. Load Training Data
    # We select the features (X) and target (y) from our trip_segments table
    print("Loading dataset from SQL...")
    query = """
    SELECT 
        route_id,
        direction_id,
        start_time_sec,   -- Time of day (feature)
        from_stop_id,     -- Origin (feature)
        to_stop_id,       -- Destination (feature)
        duration_sec      -- TARGET VARIABLE
    FROM trip_segments
    WHERE duration_sec > 0 AND duration_sec < 3600 -- Filter outliers (>1 hour segments)
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("Error: No training data found.")
        return

    # 2. Preprocessing
    print(f"Dataset loaded: {len(df)} rows.")
    
    # Encode Categorical Variables (route_id, stop_ids)
    # Models need numbers, not strings like 'A' or '101N'
    le_route = LabelEncoder()
    df['route_encoded'] = le_route.fit_transform(df['route_id'])
    
    le_stop = LabelEncoder()
    # Fit on all unique stops to ensure coverage
    all_stops = pd.concat([df['from_stop_id'], df['to_stop_id']]).unique()
    le_stop.fit(all_stops)
    
    df['from_encoded'] = le_stop.transform(df['from_stop_id'])
    df['to_encoded'] = le_stop.transform(df['to_stop_id'])

    # Define Features (X) and Target (y)
    X = df[['route_encoded', 'direction_id', 'start_time_sec', 'from_encoded', 'to_encoded']]
    y = df['duration_sec']

    # 3. Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 4. Model Training (Random Forest)
    print("Training Random Forest Regressor (this may take a minute)...")
    model = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    # 5. Evaluation
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print("\n--- Model Evaluation Results ---")
    print(f"Mean Absolute Error (MAE): {mae:.2f} seconds")
    print(f"R² Score: {r2:.4f}")
    
    if r2 > 0.9:
        print("Model Performance: Excellent")
    elif r2 > 0.7:
        print("Model Performance: Good")
    else:
        print("Model Performance: Needs Improvement")

    # 6. Save Model
    joblib.dump(model, 'subway_time_model.pkl')
    print("\nModel saved to 'subway_time_model.pkl'")

if __name__ == "__main__":
    train_travel_time_model()