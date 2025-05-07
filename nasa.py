import streamlit as st
import requests
import pymysql
from datetime import datetime, date
import pandas as pd


API_KEY = "ZVKht9INyTZ8SmhgjjzUKb0mQJEnB6YH27WEqUSL"
url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date=2024-01-01&end_date=2024-01-08&api_key={API_KEY}"


asteroids_data = []
target = 10000

while len(asteroids_data) < target:
    response = requests.get(url)
    data = response.json()
    details = data['near_earth_objects']
    for approach_date, info in details.items():
        for i in info:
            asteroids_data.append(dict(
                id=int(i['id']),
                neo_reference_id=int(i['neo_reference_id']),
                name=i['name'],
                absolute_magnitude_h=float(i['absolute_magnitude_h']),
                estimated_diameter_min_km=float(i['estimated_diameter']['kilometers']['estimated_diameter_min']),
                estimated_diameter_max_km=float(i['estimated_diameter']['kilometers']['estimated_diameter_max']),
                is_potentially_hazardous_asteroid=bool(i['is_potentially_hazardous_asteroid']),
                close_approach_date=datetime.strptime(i['close_approach_data'][0]['close_approach_date'], "%Y-%m-%d").date(),
                relative_velocity_kmph=float(i['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']),
                astronomical=float(i['close_approach_data'][0]['miss_distance']['astronomical']),
                miss_distance_km=float(i['close_approach_data'][0]['miss_distance']['kilometers']),
                miss_distance_lunar=float(i['close_approach_data'][0]['miss_distance']['lunar']),
                orbiting_body=i['close_approach_data'][0]['orbiting_body']
            ))
            if len(asteroids_data) >= target:
                break
        if len(asteroids_data) >= target:
            break
    url = data['links'].get('next')
    

# MySQL Connection
connection = pymysql.connect(
   host="localhost",
   user="root",
   password="Karthik9150",
   database="astro",
)
cursor = connection.cursor()

st.title("🚀 NASA Near-Earth Object (NEO) Tracking")
# Filters Section
st.sidebar.header("Filter Options")
start_date = st.sidebar.date_input("Start Close Approach Date", value=date(2024, 1, 1))
end_date = st.sidebar.date_input("End Close Approach Date", value=date(2024, 12, 31))
min_au = st.sidebar.slider("Min Astronomical Distance (AU)", 0.0, 1.0, 0.0)
max_au = st.sidebar.slider("Max Astronomical Distance (AU)", 0.0, 1.0, 1.0)
min_ld = st.sidebar.slider("Min Lunar Distance (LD)", 0.0, 100.0, 0.0)
max_ld = st.sidebar.slider("Max Lunar Distance (LD)", 0.0, 100.0, 100.0)
min_velocity = st.sidebar.number_input("Min Velocity (km/h)", value=0.0)
min_diameter = st.sidebar.number_input("Min Estimated Diameter (km)", value=0.0)
max_diameter = st.sidebar.number_input("Max Estimated Diameter (km)", value=10.0)
hazardous_only = st.sidebar.checkbox("Only Potentially Hazardous")

# Dropdown for Queries
queries = [
    "Count how many times each asteroid has approached Earth",
    "Average velocity of each asteroid over multiple approaches",
    "List top 10 fastest asteroids",
    "Find potentially hazardous asteroids that have approached Earth more than 3 times",
    "Find the month with the most asteroid approaches",
    "Get the asteroid with the fastest ever approach speed",
    "Sort asteroids by maximum estimated diameter (descending)",
    "Asteroid whose closest approach is getting nearer over time",
    "Name, date and miss distance of closest approach to Earth",
    "Asteroids that approached Earth with velocity > 50,000 km/h",
    "Count of approaches per month",
    "Asteroid with highest brightness (lowest magnitude)",
    "Number of hazardous vs non-hazardous asteroids",
    "Asteroids that passed closer than the Moon (< 1 LD)",
    "Asteroids that came within 0.05 AU"
]
selected_query = st.selectbox("Select a Query to Run", queries)

# SQL Queries
query_map = {
    queries[0]: """
        SELECT a.name, COUNT(*) AS approach_count
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
    """,
    queries[1]: """
        SELECT a.name, AVG(c.relative_velocity_kmph) AS avg_velocity
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
    """,
    queries[2]: """
        SELECT a.name, MAX(c.relative_velocity_kmph) AS max_velocity
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
        ORDER BY max_velocity DESC
        LIMIT 10
    """,
    queries[3]: """
        SELECT a.name, COUNT(*) AS approach_count
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE a.is_potentially_hazardous_asteroid = TRUE
        GROUP BY a.name
        HAVING approach_count > 3
    """,
    queries[4]: """
        SELECT MONTH(close_approach_date) AS month, COUNT(*) AS approach_count
        FROM close_approach
        GROUP BY month
        ORDER BY approach_count DESC
        LIMIT 1
    """,
    queries[5]: """
        SELECT a.name, MAX(c.relative_velocity_kmph) AS max_velocity
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
        ORDER BY max_velocity DESC
        LIMIT 1
    """,
    queries[6]: """
        SELECT name, estimated_diameter_max_km
        FROM asteroids
        ORDER BY estimated_diameter_max_km DESC
    """,
    queries[7]: """
        SELECT name, close_approach_date, miss_distance_km
        FROM close_approach
        JOIN (
            SELECT neo_reference_id, COUNT(*) AS freq
            FROM close_approach
            GROUP BY neo_reference_id
            ORDER BY freq DESC
            LIMIT 1
        ) t ON close_approach.neo_reference_id = t.neo_reference_id
        ORDER BY close_approach_date
    """,
    queries[8]: """
        SELECT a.name, c.close_approach_date, c.miss_distance_km
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        ORDER BY c.miss_distance_km ASC
        LIMIT 1
    """,
    queries[9]: """
        SELECT DISTINCT a.name
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE c.relative_velocity_kmph > 50000
    """,
    queries[10]: """
        SELECT MONTH(close_approach_date) AS month, COUNT(*) AS count
        FROM close_approach
        GROUP BY month
    """,
    queries[11]: """
        SELECT name, MIN(absolute_magnitude_h) AS brightness
        FROM asteroids
    """,
    queries[12]: """
        SELECT is_potentially_hazardous_asteroid, COUNT(*) AS count
        FROM asteroids
        GROUP BY is_potentially_hazardous_asteroid
    """,
    queries[13]: """
        SELECT a.name, c.close_approach_date, c.miss_distance_lunar
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE c.miss_distance_lunar < 1
    """,
    queries[14]: """
        SELECT a.name, c.close_approach_date, c.astronomical
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE c.astronomical < 0.05
    """
}

# Run Query
if selected_query in query_map:
    cursor.execute(query_map[selected_query])
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
    st.dataframe(df)
else:
    st.warning("Select a valid query from the dropdown.")