import folium
import pandas as pd
from folium.plugins import HeatMap

def create_base_map(center_lat=39.8283, center_lon=-98.5795, zoom_start=4):
    """
    Initializes a highly modern, dark-themed Folium map (Apple Maps Dark aesthetic).
    We use CartoDB Dark Matter as the clean, minimalist basemap.
    """
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom_start,
        tiles='CartoDB dark_matter',
        control_scale=False,
        zoom_control=True
    )
    return m

def add_demand_heatmap(m, data: pd.DataFrame, weight_col='demand_score'):
    """
    Adds a heatmap layer representing the Predictive Demand Score.
    """
    if data.empty or weight_col not in data.columns:
        return m
        
    # HeatMap takes list of [lat, lon, weight]
    heat_data = [[row['lat'], row['lon'], row[weight_col]] for index, row in data.iterrows() if pd.notnull(row['lat']) and pd.notnull(row['lon'])]
    
    # Custom vibrant gradient: deep violet -> cyan -> electric blue
    gradient = {
        0.2: '#4B0082', # Indigo
        0.4: '#8A2BE2', # BlueViolet
        0.6: '#00BFFF', # DeepSkyBlue
        0.8: '#00FFFF', # Cyan
        1.0: '#E0FFFF'  # LightCyan
    }
    
    HeatMap(
        heat_data,
        radius=18,
        blur=15,
        max_zoom=10,
        gradient=gradient
    ).add_to(m)
    
    return m

def add_permit_pulse(m, data: pd.DataFrame):
    """
    Adds circles for active building permits.
    These act as 'pulses' using Folium CircleMarkers with cyan borders.
    """
    for _, row in data.iterrows():
        if pd.notnull(row['lat']) and pd.notnull(row['lon']) and pd.notnull(row['units']):
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=row['units'] / 5 + 3, # Scale visually
                color='#0A84FF', # iOS Blue
                fill=True,
                fill_color='#0A84FF',
                fill_opacity=0.4,
                weight=2,
                tooltip=f"ZIP: {row['zip_code']} | Permits: {row['units']} units"
            ).add_to(m)
    return m

def add_disaster_zones(m, data: pd.DataFrame):
    """
    Adds red overlays for FEMA active zones.
    """
    for _, row in data.iterrows():
        if pd.notnull(row['lat']) and pd.notnull(row['lon']):
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=25,
                color='#FF3B30', # iOS Red
                fill=True,
                fill_color='#FF3B30',
                fill_opacity=0.3,
                weight=2,
                tooltip=f"Disaster Zone: {row.get('city', 'Unknown')} (ZIP: {row['zip_code']})"
            ).add_to(m)
    return m
