import streamlit as st
from streamlit_folium import st_folium
import sqlite3
import pandas as pd
import os
import map_renderer as renderer

st.set_page_config(
    page_title="StorageSense National",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load minimal CSS
def load_css():
    css_path = os.path.join(os.path.dirname(__file__), "style.css")
    if os.path.exists(css_path):
        with open(css_path) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
            
load_css()

# DB connection logic (Secure fallback)
try:
    # Attempt to load Supabase / Production PostgreSQL from Streamlit Secrets
    DB_URI = st.secrets["SUPABASE_DATABASE_URI"]
    IS_SQLITE = False
except (Exception, KeyError):
    # Fallback to local SQLite for desktop prototyping
    DB_URI = os.path.join(os.path.dirname(__file__), "..", "db", "storagesense.db")
    IS_SQLITE = True

# --- Auto-Initialize Minimal Map Data for Cloud Deployments ---
def ensure_fallback_db():
    if IS_SQLITE and not os.path.exists(DB_URI):
        try:
            with sqlite3.connect(DB_URI) as conn:
                schema_path = os.path.join(os.path.dirname(__file__), "..", "db", "schema.sql")
                if os.path.exists(schema_path):
                    with open(schema_path) as f:
                        conn.executescript(f.read())
                
                # Insert mock data so the map is never completely blank when deployed without a production database
                mock_zips = [
                    ("78701", "48453", "TX", "Austin", 30.270, -97.741),
                    ("33139", "12086", "FL", "Miami", 25.790, -80.140),
                    ("80202", "08031", "CO", "Denver", 39.754, -104.998),
                    ("60601", "17031", "IL", "Chicago", 41.885, -87.622),
                    ("97204", "41051", "OR", "Portland", 45.518, -122.678)
                ]
                import random
                for z, fips, st, city, lat, lon in mock_zips:
                    conn.execute("INSERT OR IGNORE INTO geography (zip_code, county_fips, state_abbr, city, lat, lon) VALUES (?, ?, ?, ?, ?, ?)", (z, fips, st, city, lat, lon))
                    conn.execute("INSERT OR REPLACE INTO local_building_permits (zip_code, permit_date, units, type) VALUES (?, DATE('now'), ?, 'Multifamily')", (z, random.randint(5, 50)))
                    conn.execute("INSERT OR REPLACE INTO behavioral_signals (zip_code, date, usps_net_migration, google_search_index, fema_disaster_active) VALUES (?, DATE('now'), 0, 0, ?)", (z, random.choice([0, 1])))
                    conn.execute("INSERT OR REPLACE INTO demand_scores (zip_code, liquidity_score_l, movement_score_m, structural_score_s, competition_score_c, final_score) VALUES (?, 0.5, 1.0, 0.5, 0.5, ?)", (z, random.random()))
        except Exception as e:
            st.error(f"Fallback database initialization error: {e}")

if IS_SQLITE:
    ensure_fallback_db()

@st.cache_data
def load_map_data(timeline_offset_months=0):
    """
    Loads geometries, scores, permits, and disasters for the given timeline offset.
    (Mocked logic incorporating timeline_offset_months for future prediction)
    """
    try:
        # Use sqlite3 module for local, psycopg2/sqlalchemy would be used for Supabase.
        # Keeping sqlite for MVP to run without DB dependencies installed.
        with sqlite3.connect(DB_URI) as conn:
            # Fetch final scores and parameters mapped through the Logic Engine
            query = '''
            SELECT 
                g.zip_code, g.city, g.lat, g.lon, 
                p.units,
                COALESCE(s.final_score, 0) AS demand_score,
                COALESCE(b.fema_disaster_active, 0) AS disaster_active
            FROM geography g
            LEFT JOIN local_building_permits p ON g.zip_code = p.zip_code AND p.permit_date = DATE('now')
            LEFT JOIN demand_scores s ON g.zip_code = s.zip_code
            LEFT JOIN behavioral_signals b ON g.zip_code = b.zip_code AND b.date = DATE('now')
            '''
            df = pd.read_sql_query(query, conn)
            
            if timeline_offset_months > 0:
                # Simulate demand scoring changes in the future base on permits
                df['demand_score'] = df['demand_score'] + (df['units'].fillna(0) * timeline_offset_months * 0.005)
                df['demand_score'] = df['demand_score'].clip(upper=1.0)
                
            return df
    except Exception as e:
        # Fallback empty dataframe if DB not initialized
        st.error(f"Error loading data: {e}. Please run initializing script.")
        return pd.DataFrame(columns=['zip_code', 'city', 'lat', 'lon', 'units', 'demand_score', 'disaster_active'])

# --- SIDEBAR UI ---
st.sidebar.markdown("## StorageSense National")
st.sidebar.markdown("### The External Signal Dashboard")
st.sidebar.markdown("Monitor upstream life events predicting self-storage demand.")

st.sidebar.subheader("Layer Controls")
show_heatmap = st.sidebar.checkbox("Demand Heatmap", value=True)
show_permits = st.sidebar.checkbox("Permit Pulse (Supply)", value=True)
show_disasters = st.sidebar.checkbox("Active Disaster Zones", value=False)

st.sidebar.subheader("Time Horizon")
timeline_months = st.sidebar.slider("Timeline Slider (Months)", min_value=0, max_value=12, value=0, step=1)

# --- MAIN UI ---
st.title("Demand Havens Map")

data = load_map_data(timeline_months)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Monitored ZIP Codes", len(data), delta=None)
with col2:
    st.metric("Total Active Permits", int(data['units'].sum()) if not data.empty and 'units' in data.columns else 0, delta=None)
with col3:
    avg_score = (data['demand_score'].mean() * 100) if not data.empty and 'demand_score' in data.columns else 0
    st.metric("Average Demand Score", f"{avg_score:.1f}/100", delta="+1.2%" if timeline_months > 0 else None)

m = renderer.create_base_map()

if not data.empty:
    if show_heatmap:
        m = renderer.add_demand_heatmap(m, data, weight_col='demand_score')
    if show_permits:
        m = renderer.add_permit_pulse(m, data)
    if show_disasters:
        disaster_data = data[data['disaster_active'] == 1]
        m = renderer.add_disaster_zones(m, disaster_data)

st_data = st_folium(m, width=1200, height=600)
